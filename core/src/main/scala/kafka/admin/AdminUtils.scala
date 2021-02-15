/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.admin

import kafka.common._
import kafka.cluster.Broker
import kafka.log.LogConfig
import kafka.server.ConfigType
import kafka.utils._
import kafka.utils.ZkUtils._

import java.util.Random
import java.util.Properties
import org.apache.kafka.common.Node
import org.apache.kafka.common.errors.{ReplicaNotAvailableException, InvalidTopicException, LeaderNotAvailableException}
import org.apache.kafka.common.protocol.{Errors, SecurityProtocol}
import org.apache.kafka.common.requests.MetadataResponse

import scala.collection._
import JavaConverters._
import mutable.ListBuffer
import scala.collection.mutable
import collection.Map
import collection.Set
import org.I0Itec.zkclient.exception.ZkNodeExistsException

object AdminUtils extends Logging {
  val rand = new Random
  val AdminClientId = "__admin_client"
  val EntityConfigChangeZnodePrefix = "config_change_"

  /**
   * There are 3 goals of replica assignment:
   *
   * 1. Spread the replicas evenly among brokers.
   * 2. For partitions assigned to a particular broker, their other replicas are spread over the other brokers.
   * 3. If all brokers have rack information, assign the replicas for each partition to different racks if possible
   *
   * To achieve this goal for replica assignment without considering racks, we:
   * 1. Assign the first replica of each partition by round-robin, starting from a random position in the broker list.
   * 2. Assign the remaining replicas of each partition with an increasing shift.
   *
   * Here is an example of assigning
   * broker-0  broker-1  broker-2  broker-3  broker-4
   * p0        p1        p2        p3        p4       (1st replica)
   * p5        p6        p7        p8        p9       (1st replica)
   * p4        p0        p1        p2        p3       (2nd replica)
   * p8        p9        p5        p6        p7       (2nd replica)
   * p3        p4        p0        p1        p2       (3nd replica)
   * p7        p8        p9        p5        p6       (3nd replica)
   *
   * To create rack aware assignment, this API will first create a rack alternated broker list. For example,
   * from this brokerID -> rack mapping:
   *
   * 0 -> "rack1", 1 -> "rack3", 2 -> "rack3", 3 -> "rack2", 4 -> "rack2", 5 -> "rack1"
   *
   * The rack alternated list will be:
   *
   * 0, 3, 1, 5, 4, 2
   *
   * Then an easy round-robin assignment can be applied. Assume 6 partitions with replication factor of 3, the assignment
   * will be:
   *
   * 0 -> 0,3,1
   * 1 -> 3,1,5
   * 2 -> 1,5,4
   * 3 -> 5,4,2
   * 4 -> 4,2,0
   * 5 -> 2,0,3
   *
   * Once it has completed the first round-robin, if there are more partitions to assign, the algorithm will start
   * shifting the followers. This is to ensure we will not always get the same set of sequences.
   * In this case, if there is another partition to assign (partition #6), the assignment will be:
   *
   * 6 -> 0,4,2 (instead of repeating 0,3,1 as partition 0)
   *
   * The rack aware assignment always chooses the 1st replica of the partition using round robin on the rack alternated
   * broker list. For rest of the replicas, it will be biased towards brokers on racks that do not have
   * any replica assignment, until every rack has a replica. Then the assignment will go back to round-robin on
   * the broker list.
   *
   * As the result, if the number of replicas is equal to or greater than the number of racks, it will ensure that
   * each rack will get at least one replica. Otherwise, each rack will get at most one replica. In a perfect
   * situation where the number of replicas is the same as the number of racks and each rack has the same number of
   * brokers, it guarantees that the replica distribution is even across brokers and racks.
   *
   * @return a Map from partition id to replica ids
   * @throws AdminOperationException If rack information is supplied but it is incomplete, or if it is not possible to
   *                                 assign each replica to a unique rack.
   *
   */
  // key是分区编号，value是brokerId集合
  def assignReplicasToBrokers(brokerMetadatas: Seq[BrokerMetadata],// 所有可用Broker的元数据
                              nPartitions: Int,// 分区数
                              replicationFactor: Int,// 副本数
                              fixedStartIndex: Int = -1,// 起始索引，第一个Broker的位置
                              startPartitionId: Int = -1): Map[Int, Seq[Int]] = {// 起始索引，第一个分区的位置
    // 校验
    if (nPartitions <= 0)
      throw new AdminOperationException("number of partitions must be larger than 0")
    if (replicationFactor <= 0)
      throw new AdminOperationException("replication factor must be larger than 0")
    if (replicationFactor > brokerMetadatas.size)
      throw new AdminOperationException(s"replication factor: $replicationFactor larger than available brokers: ${brokerMetadatas.size}")
    // 如果传递过来的所有可用Broker的机架信息都为空
    // 机架信息如何来的？可以参考：kafkaServer创建的KafkaHealthcheck
    if (brokerMetadatas.forall(_.rack.isEmpty))
      // 分配副本，没有机架信息
      assignReplicasToBrokersRackUnaware(nPartitions, replicationFactor, brokerMetadatas.map(_.id), fixedStartIndex,
        startPartitionId)
    // 机架信息不为空
    else {
      if (brokerMetadatas.exists(_.rack.isEmpty))
        throw new AdminOperationException("Not all brokers have rack information for replica rack aware assignment")
      // 分配副本，有机架信息
      assignReplicasToBrokersRackAware(nPartitions, replicationFactor, brokerMetadatas, fixedStartIndex,
        startPartitionId)
    }
  }

  // 分配副本，没有机架信息，副本其实也就是Broker，也就是一台kafkaServer
  // 返回类型为map，key是分区编号，value是brokerId集合
  private def assignReplicasToBrokersRackUnaware(nPartitions: Int,// 分区数
                                                 replicationFactor: Int,// 副本数
                                                 brokerList: Seq[Int],// 所有可用Broker的id
                                                 fixedStartIndex: Int,// 起始索引，第一个副本的位置
                                                 startPartitionId: Int): Map[Int, Seq[Int]] = {// 起始分区编号，比如在创建topic时创建三个分区：0,1,2，这个可能是从1开始
    // 保存分配结果
    val ret = mutable.Map[Int, Seq[Int]]()
    // brokerId数组
    val brokerArray = brokerList.toArray
    // 计算获取副本的起始索引
    val startIndex = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokerArray.length)
    // 计算获取起始的分区id,保证currentPartitionId >= 0
    var currentPartitionId = math.max(0, startPartitionId)
    // 计算副本之间的间隔
    var nextReplicaShift = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokerArray.length)
    // 遍历分区，一个分区对应replicationFactor个副本
    for (_ <- 0 until nPartitions) {
      // 从第二分区开始 && 当前分区的编号为brokerId集合的倍数时才去递增下一个副本的间隔(也就是分配的分区数，超过了broker的数量时)
      if (currentPartitionId > 0 && (currentPartitionId % brokerArray.length == 0))
        nextReplicaShift += 1
      // 计算副本的起始索引
      val firstReplicaIndex = (currentPartitionId + startIndex) % brokerArray.length
      // 获取第一个副本对应的BrokerId
      val replicaBuffer = mutable.ArrayBuffer(brokerArray(firstReplicaIndex))
      // 遍历应该分配的副本数，注意由于上面获取了一个副本了，所以这里需要replicationFactor - 1
      for (j <- 0 until replicationFactor - 1)
        // 计算副本下标，然后获取副本对应的BrokerId
        replicaBuffer += brokerArray(replicaIndex(firstReplicaIndex, nextReplicaShift, j, brokerArray.length))
      // 保存分配结果
      ret.put(currentPartitionId, replicaBuffer)
      // 计算下一个分区
      currentPartitionId += 1
    }
    ret
  }

  // 分配副本，有机架信息
  // 返回类型为map，key是分区编号，value是brokerId集合
  private def assignReplicasToBrokersRackAware(nPartitions: Int,// 分区数
                                               replicationFactor: Int,// 副本数
                                               brokerMetadatas: Seq[BrokerMetadata],// 所有可用Broker的元数据
                                               fixedStartIndex: Int,// 起始索引，第一个副本的位置
                                               startPartitionId: Int): Map[Int, Seq[Int]] = {// 起始分区编号，比如在创建topic时创建三个分区：0,1,2，这个可能是从1开始
    // 获取brokerId和对应的rack信息
    val brokerRackMap = brokerMetadatas.collect { case BrokerMetadata(id, Some(rack)) =>
      id -> rack
    }.toMap
    // rack的数量
    val numRacks = brokerRackMap.values.toSet.size
    // 根据rack将brokerId进行分组，然后交替获取rack对应的brokerId集合上的数据
    val arrangedBrokerList = getRackAlternatedBrokerList(brokerRackMap)
    // broekr数量
    val numBrokers = arrangedBrokerList.size
    // 记录结果
    val ret = mutable.Map[Int, Seq[Int]]()
    // 计算获取副本的起始索引
    val startIndex = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(arrangedBrokerList.size)
    // 保证startPartitionId >= 0
    var currentPartitionId = math.max(0, startPartitionId)
    // 计算副本之间的间隔
    var nextReplicaShift = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(arrangedBrokerList.size)
    for (_ <- 0 until nPartitions) {
      if (currentPartitionId > 0 && (currentPartitionId % arrangedBrokerList.size == 0))
        nextReplicaShift += 1
      // 计算副本的起始索引
      val firstReplicaIndex = (currentPartitionId + startIndex) % arrangedBrokerList.size
      // 获取leader
      val leader = arrangedBrokerList(firstReplicaIndex)
      // 保存到集合中，集合是分区的副本集合
      val replicaBuffer = mutable.ArrayBuffer(leader)
      // 获取leader的rack信息并保存到集合中，集合是分区的机架信息集合
      val racksWithReplicas = mutable.Set(brokerRackMap(leader))
      // 分区的副本集合
      val brokersWithReplicas = mutable.Set(leader)
      var k = 0
      for (_ <- 0 until replicationFactor - 1) {
        var done = false
        while (!done) {
          val broker = arrangedBrokerList(replicaIndex(firstReplicaIndex, nextReplicaShift * numRacks, k, arrangedBrokerList.size))
          val rack = brokerRackMap(broker)
          // Skip this broker if
          // 1. there is already a broker in the same rack that has assigned a replica AND there is one or more racks
          //    that do not have any replica, or
          // 2. the broker has already assigned a replica AND there is one or more brokers that do not have replica assigned
          if ((!racksWithReplicas.contains(rack) || racksWithReplicas.size == numRacks)
              && (!brokersWithReplicas.contains(broker) || brokersWithReplicas.size == numBrokers)) {
            replicaBuffer += broker
            racksWithReplicas += rack
            brokersWithReplicas += broker
            done = true
          }
          k += 1
        }
      }
      ret.put(currentPartitionId, replicaBuffer)
      currentPartitionId += 1
    }
    ret
  }

  /**
    * Given broker and rack information, returns a list of brokers alternated by the rack. Assume
    * this is the rack and its brokers:
    *
    * rack1: 0, 1, 2
    * rack2: 3, 4, 5
    * rack3: 6, 7, 8
    *
    * This API would return the list of 0, 3, 6, 1, 4, 7, 2, 5, 8
    *
    * This is essential to make sure that the assignReplicasToBrokers API can use such list and
    * assign replicas to brokers in a simple round-robin fashion, while ensuring an even
    * distribution of leader and replica counts on each broker and that replicas are
    * distributed to all racks.
    */
  // 参数传递进来map，key是brokerId，value是rack信息
  // 返回信息是brokerId集合
  private[admin] def getRackAlternatedBrokerList(brokerRackMap: Map[Int, String]): IndexedSeq[Int] = {
    // 翻转map，将机架信息相同的brokerId归类
    // 参数：key是brokerId，value是rack信息
    // 结果：key是rack信息，value是brokerId集合
    val brokersIteratorByRack = getInverseMap(brokerRackMap).map { case (rack, brokers) =>
      (rack, brokers.toIterator)
    }
    // 获取所有的racks并排序
    val racks = brokersIteratorByRack.keys.toArray.sorted
    // 记录返回的结果
    val result = new mutable.ArrayBuffer[Int]
    var rackIndex = 0
    while (result.size < brokerRackMap.size) {
      // 获取rack对应的所有brokerId
      val rackIterator = brokersIteratorByRack(racks(rackIndex))
      // 遍历这些brokerId
      if (rackIterator.hasNext)
        result += rackIterator.next()
      rackIndex = (rackIndex + 1) % racks.length
    }
    result
  }

  // 翻转map，将机架信息相同的brokerId归类
  // 参数：key是brokerId，value是rack信息
  // 结果：key是rack信息，value是brokerId集合
  private[admin] def getInverseMap(brokerRackMap: Map[Int, String]): Map[String, Seq[Int]] = {
    brokerRackMap.toSeq.map { case (id, rack) => (rack, id) }
      .groupBy { case (rack, _) => rack }
      .map { case (rack, rackAndIdList) => (rack, rackAndIdList.map { case (_, id) => id }.sorted) }
  }
 /**
  * Add partitions to existing topic with optional replica assignment
  *
  * @param zkUtils Zookeeper utilities
  * @param topic Topic for adding partitions to
  * @param numPartitions Number of partitions to be set
  * @param replicaAssignmentStr Manual replica assignment
  * @param checkBrokerAvailable Ignore checking if assigned replica broker is available. Only used for testing
  */
 // 添加分区
  def addPartitions(zkUtils: ZkUtils,
                    topic: String,
                    numPartitions: Int = 1,// --partitions 配置
                    replicaAssignmentStr: String = "",// --replica-assignment 配置
                    checkBrokerAvailable: Boolean = true,
                    rackAwareMode: RackAwareMode = RackAwareMode.Enforced) {
   // 获取topic各个分区的副本分配结果，返回类型Map[TopicAndPartition, Seq[Int]]
    val existingPartitionsReplicaList = zkUtils.getReplicaAssignmentForTopics(List(topic))
   // topic不存在
    if (existingPartitionsReplicaList.size == 0)
      throw new AdminOperationException("The topic %s does not exist".format(topic))

    val existingReplicaListForPartitionZero = existingPartitionsReplicaList.find(p => p._1.partition == 0) match {
      case None => throw new AdminOperationException("the topic does not have partition with id 0, it should never happen")
      case Some(headPartitionReplica) => headPartitionReplica._2
    }
   // 分区数只能增加不能减少，从这里可以看出--partitions 是一定需要配置的
    val partitionsToAdd = numPartitions - existingPartitionsReplicaList.size
    if (partitionsToAdd <= 0)
      throw new AdminOperationException("The number of partitions for a topic can only be increased")

    // create the new partition replication list
   // 获取所有broker的元数据BrokerMetadata
    val brokerMetadatas = getBrokerMetadatas(zkUtils, rackAwareMode)
   // 获取分配结果Map[Int, Seq[Int]]
   // key是分区编号，value是brokerId集合
    val newPartitionReplicaList =
      // 没有指定 --replica-assignment 配置
      if (replicaAssignmentStr == null || replicaAssignmentStr == "") {
        // 计算起始索引
        val startIndex = math.max(0, brokerMetadatas.indexWhere(_.id >= existingReplicaListForPartitionZero.head))
        // 执行重分配
        AdminUtils.assignReplicasToBrokers(brokerMetadatas, partitionsToAdd, existingReplicaListForPartitionZero.size,
          startIndex, existingPartitionsReplicaList.size)
      }
      else
        // 如果用户指定了---replica-assignment 配置那么解析手动分配的结果
        getManualReplicaAssignment(replicaAssignmentStr, brokerMetadatas.map(_.id).toSet,
          existingPartitionsReplicaList.size, checkBrokerAvailable)

    // check if manual assignment has the right replication factor
    val unmatchedRepFactorList = newPartitionReplicaList.values.filter(p => (p.size != existingReplicaListForPartitionZero.size))
    if (unmatchedRepFactorList.size != 0)
      throw new AdminOperationException("The replication factor in manual replication assignment " +
        " is not equal to the existing replication factor for the topic " + existingReplicaListForPartitionZero.size)

    info("Add partition list for %s is %s".format(topic, newPartitionReplicaList))
    val partitionReplicaList = existingPartitionsReplicaList.map(p => p._1.partition -> p._2)
    // add the new list
    partitionReplicaList ++= newPartitionReplicaList
   // 将分配结果写入zk
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, partitionReplicaList, update = true)
  }

  /**
    * 解析手动分配结果
    * @param replicaAssignmentList 为 --replica-assignment 配置
    * @param availableBrokerList 所有brokerId集合
    * @param startPartitionId 起始分区索引下标
    * @param checkBrokerAvailable 是否需要检查broker的状态
    * @return
    */
  def getManualReplicaAssignment(replicaAssignmentList: String, availableBrokerList: Set[Int], startPartitionId: Int, checkBrokerAvailable: Boolean = true): Map[Int, List[Int]] = {
    var partitionList = replicaAssignmentList.split(",")
    val ret = new mutable.HashMap[Int, List[Int]]()
    var partitionId = startPartitionId
    partitionList = partitionList.takeRight(partitionList.size - partitionId)
    for (i <- 0 until partitionList.size) {
      val brokerList = partitionList(i).split(":").map(s => s.trim().toInt)
      if (brokerList.size <= 0)
        throw new AdminOperationException("replication factor must be larger than 0")
      if (brokerList.size != brokerList.toSet.size)
        throw new AdminOperationException("duplicate brokers in replica assignment: " + brokerList)
      if (checkBrokerAvailable && !brokerList.toSet.subsetOf(availableBrokerList))
        throw new AdminOperationException("some specified brokers not available. specified brokers: " + brokerList.toString +
          "available broker:" + availableBrokerList.toString)
      ret.put(partitionId, brokerList.toList)
      if (ret(partitionId).size != ret(startPartitionId).size)
        throw new AdminOperationException("partition " + i + " has different replication factor: " + brokerList)
      partitionId = partitionId + 1
    }
    ret.toMap
  }

  def deleteTopic(zkUtils: ZkUtils, topic: String) {
    try {
      zkUtils.createPersistentPath(getDeleteTopicPath(topic))
    } catch {
      case e1: ZkNodeExistsException => throw new TopicAlreadyMarkedForDeletionException(
        "topic %s is already marked for deletion".format(topic))
      case e2: Throwable => throw new AdminOperationException(e2.toString)
    }
  }

  def isConsumerGroupActive(zkUtils: ZkUtils, group: String) = {
    zkUtils.getConsumersInGroup(group).nonEmpty
  }

  /**
   * Delete the whole directory of the given consumer group if the group is inactive.
   *
   * @param zkUtils Zookeeper utilities
   * @param group Consumer group
   * @return whether or not we deleted the consumer group information
   */
  def deleteConsumerGroupInZK(zkUtils: ZkUtils, group: String) = {
    if (!isConsumerGroupActive(zkUtils, group)) {
      val dir = new ZKGroupDirs(group)
      zkUtils.deletePathRecursive(dir.consumerGroupDir)
      true
    }
    else false
  }

  /**
   * Delete the given consumer group's information for the given topic in Zookeeper if the group is inactive.
   * If the consumer group consumes no other topics, delete the whole consumer group directory.
   *
   * @param zkUtils Zookeeper utilities
   * @param group Consumer group
   * @param topic Topic of the consumer group information we wish to delete
   * @return whether or not we deleted the consumer group information for the given topic
   */
  def deleteConsumerGroupInfoForTopicInZK(zkUtils: ZkUtils, group: String, topic: String) = {
    val topics = zkUtils.getTopicsByConsumerGroup(group)
    if (topics == Seq(topic)) {
      deleteConsumerGroupInZK(zkUtils, group)
    }
    else if (!isConsumerGroupActive(zkUtils, group)) {
      val dir = new ZKGroupTopicDirs(group, topic)
      zkUtils.deletePathRecursive(dir.consumerOwnerDir)
      zkUtils.deletePathRecursive(dir.consumerOffsetDir)
      true
    }
    else false
  }

  /**
   * Delete every inactive consumer group's information about the given topic in Zookeeper.
   *
   * @param zkUtils Zookeeper utilities
   * @param topic Topic of the consumer group information we wish to delete
   */
  def deleteAllConsumerGroupInfoForTopicInZK(zkUtils: ZkUtils, topic: String) {
    val groups = zkUtils.getAllConsumerGroupsForTopic(topic)
    groups.foreach(group => deleteConsumerGroupInfoForTopicInZK(zkUtils, group, topic))
  }

  def topicExists(zkUtils: ZkUtils, topic: String): Boolean =
    zkUtils.zkClient.exists(getTopicPath(topic))

  // 获取所有broker的元数据BrokerMetadata
  // rackAwareMode默认为Enforced
  def getBrokerMetadatas(zkUtils: ZkUtils, rackAwareMode: RackAwareMode = RackAwareMode.Enforced,
                        brokerList: Option[Seq[Int]] = None): Seq[BrokerMetadata] = {
    // 获取所有的broker集合
    val allBrokers = zkUtils.getAllBrokersInCluster()
    // 从allBrokers集合中过滤出包含在brokerList集合中的Broker，如果brokerList为空那么返回allBrokers
    // 这里获取的broker就是下面要进行分区副本分配被候选的broker
    val brokers = brokerList.map(brokerIds => allBrokers.filter(b => brokerIds.contains(b.id))).getOrElse(allBrokers)
    // 过滤出包含机架信息的Broker
    val brokersWithRack = brokers.filter(_.rack.nonEmpty)
    // 这里就是验证在Enforced模式下，如果有含义机架信息的broker数量和全部broker数量不一致，那么抛出异常
    if (rackAwareMode == RackAwareMode.Enforced && brokersWithRack.nonEmpty && brokersWithRack.size < brokers.size) {
      throw new AdminOperationException("Not all brokers have rack information. Add --disable-rack-aware in command line" +
        " to make replica assignment without rack information.")
    }
    // 判断rackAwareMode模式然后获取Broker的元数据信息
    val brokerMetadatas = rackAwareMode match {
        // Disabled模式，获取brokers集合中所有Broker的元数据信息
      case RackAwareMode.Disabled => brokers.map(broker => BrokerMetadata(broker.id, None))
        // 如果是Safe，但是有机架信息的Broker和总Broker数量不一致，那么就回退到Disabled模式
      case RackAwareMode.Safe if brokersWithRack.size < brokers.size =>
        brokers.map(broker => BrokerMetadata(broker.id, None))
        // 其他请求，获取所有Broker的元数据信息
      case _ => brokers.map(broker => BrokerMetadata(broker.id, broker.rack))
    }
    // 排序
    brokerMetadatas.sortBy(_.id)
  }

  // 创建topic
  def createTopic(zkUtils: ZkUtils,
                  topic: String,// 创建的topic
                  partitions: Int,// 分区个数
                  replicationFactor: Int,// 副本个数
                  topicConfig: Properties = new Properties,// topic的一些配置参数，如 config max.message.bytes=64000、config flush.messages=1
                  rackAwareMode: RackAwareMode = RackAwareMode.Enforced) {// 默认模式Enforced
    // 获取所有broker的元数据Seq[BrokerMetadata]
    val brokerMetadatas = getBrokerMetadatas(zkUtils, rackAwareMode)
    // 创建分区副本列表，返回类型为Map[Int, Seq[Int]]，key是分区编号，value是brokerId集合
    val replicaAssignment = AdminUtils.assignReplicasToBrokers(brokerMetadatas, partitions, replicationFactor)
    // 创建或者更新topic的分区的副本列表
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, replicaAssignment, topicConfig)
  }

  // 创建或者更新topic的分区的副本列表
  def createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils: ZkUtils,
                                                     topic: String,
                                                     partitionReplicaAssignment: Map[Int, Seq[Int]],// 分区的副本分配列表，key是分区编号，value是brokerId集合
                                                     config: Properties = new Properties,// topic的一些配置参数
                                                     update: Boolean = false) {// 是否为更新topic
    // validate arguments
    Topic.validate(topic)
    require(partitionReplicaAssignment.values.map(_.size).toSet.size == 1, "All partitions should have the same number of replicas.")
    // 获取topic在zk上的路径/brokers/topics/{topic}
    val topicPath = getTopicPath(topic)
    // 新增topic操作
    if (!update) {
      // 新增topic判断是否已经存在
      if (zkUtils.zkClient.exists(topicPath))
        throw new TopicExistsException("Topic \"%s\" already exists.".format(topic))
      // 新增topic判断是否存在冲突
      else if (Topic.hasCollisionChars(topic)) {
        val allTopics = zkUtils.getAllTopics()
        val collidingTopics = allTopics.filter(t => Topic.hasCollision(topic, t))
        if (collidingTopics.nonEmpty) {
          throw new InvalidTopicException("Topic \"%s\" collides with existing topics: %s".format(topic, collidingTopics.mkString(", ")))
        }
      }
    }

    partitionReplicaAssignment.values.foreach(reps => require(reps.size == reps.toSet.size, "Duplicate replica assignment found: "  + partitionReplicaAssignment))

    // Configs only matter if a topic is being created. Changing configs via AlterTopic is not supported
    // 创建topic
    if (!update) {
      // write out the config if there is any, this isn't transactional with the partition assignments
      // 这里可以看出只有创建topic，配置才会起作用
      LogConfig.validate(config)
      // 将配置写到zk上的"/config/topics/{topic}"节点下
      // 对应节点内容如下：{"version":1,"config":{"segment.bytes":"104857600","compression.type":"producer","cleanup.policy":"compact"}}
      writeEntityConfig(zkUtils, ConfigType.Topic, topic, config)
    }

    // create the partition assignment
    // 将分配结果写或更新到zk上的"/brokers/topics/{topic}"节点下
    writeTopicPartitionAssignment(zkUtils, topic, partitionReplicaAssignment, update)
  }

  // 写或更新分区的分配结果
  // 写入完成触发kafka.controller.PartitionStateMachine.TopicChangeListener
  // 更新完成触发kafka.controller.PartitionStateMachine.PartitionModificationsListener.PartitionModificationsListener
  private def writeTopicPartitionAssignment(zkUtils: ZkUtils, topic: String, replicaAssignment: Map[Int, Seq[Int]], update: Boolean) {
    try {
      // 获取topic在zk上的路径/brokers/topics/{topic}
      val zkPath = getTopicPath(topic)
      // 转换分区的分配结果为JSON
      // 结果大体如下：{"version":1,"partitions":{"2":[0,1],"1":[0,2],"0":[1,2]}
      val jsonPartitionData = zkUtils.replicaAssignmentZkData(replicaAssignment.map(e => (e._1.toString -> e._2)))
      // 新增
      if (!update) {
        info("Topic creation " + jsonPartitionData.toString)
        // 在zk上的路径/brokers/topics/下创建一个永久节点/brokers/topics/{topic}，并将数据写入zk
        zkUtils.createPersistentPath(zkPath, jsonPartitionData)
      // 更新
      } else {
        info("Topic update " + jsonPartitionData.toString)
        // 将数据更新到zk上的/brokers/topics/{topic}节点
        zkUtils.updatePersistentPath(zkPath, jsonPartitionData)
      }
      debug("Updated path %s with %s for replica assignment".format(zkPath, jsonPartitionData))
    } catch {
      case e: ZkNodeExistsException => throw new TopicExistsException("topic %s already exists".format(topic))
      case e2: Throwable => throw new AdminOperationException(e2.toString)
    }
  }

  /**
   * Update the config for a client and create a change notification so the change will propagate to other brokers
   *
   * @param zkUtils Zookeeper utilities used to write the config to ZK
   * @param clientId: The clientId for which configs are being changed
   * @param configs: The final set of configs that will be applied to the topic. If any new configs need to be added or
   *                 existing configs need to be deleted, it should be done prior to invoking this API
   *
   */
  def changeClientIdConfig(zkUtils: ZkUtils, clientId: String, configs: Properties) {
    changeEntityConfig(zkUtils, ConfigType.Client, clientId, configs)
  }

  /**
   * Update the config for an existing topic and create a change notification so the change will propagate to other brokers
   *
   * @param zkUtils Zookeeper utilities used to write the config to ZK
   * @param topic: The topic for which configs are being changed
   * @param configs: The final set of configs that will be applied to the topic. If any new configs need to be added or
   *                 existing configs need to be deleted, it should be done prior to invoking this API
   *
   */
  // 更新现有topic的配置
  def changeTopicConfig(zkUtils: ZkUtils, topic: String, configs: Properties) {
    if(!topicExists(zkUtils, topic))
      throw new AdminOperationException("Topic \"%s\" does not exist.".format(topic))
    // remove the topic overrides
    LogConfig.validate(configs)
    changeEntityConfig(zkUtils, ConfigType.Topic, topic, configs)
  }

  private def changeEntityConfig(zkUtils: ZkUtils, entityType: String, entityName: String, configs: Properties) {
    // write the new config--may not exist if there were previously no overrides
    // 将topic配置写入zk
    writeEntityConfig(zkUtils, entityType, entityName, configs)

    // create the change notification
    // 创建一个永久有序节点并节数据写入该节点  "/config/changes/config_change_"
    val seqNode = ZkUtils.EntityConfigChangesPath + "/" + EntityConfigChangeZnodePrefix
    val content = Json.encode(getConfigChangeZnodeData(entityType, entityName))
    zkUtils.zkClient.createPersistentSequential(seqNode, content)
  }

  def getConfigChangeZnodeData(entityType: String, entityName: String) : Map[String, Any] = {
    Map("version" -> 1, "entity_type" -> entityType, "entity_name" -> entityName)
  }

  /**
   * Write out the topic config to zk, if there is any
   */
  private def writeEntityConfig(zkUtils: ZkUtils, entityType: String, entityName: String, config: Properties) {
    val configMap: mutable.Map[String, String] = {
      import JavaConversions._
      config
    }
    val map = Map("version" -> 1, "config" -> configMap)
    // 将配置写入zk /config/{entityType}/{entity}路径
    // 内容大体如下：{"version":1,"config":{"segment.bytes":"104857600","compression.type":"producer","cleanup.policy":"compact"}}
    zkUtils.updatePersistentPath(getEntityConfigPath(entityType, entityName), Json.encode(map))
  }

  /**
   * Read the entity (topic or client) config (if any) from zk
   */
  def fetchEntityConfig(zkUtils: ZkUtils, entityType: String, entity: String): Properties = {
    val str: String = zkUtils.zkClient.readData(getEntityConfigPath(entityType, entity), true)
    val props = new Properties()
    if(str != null) {
      Json.parseFull(str) match {
        case None => // there are no config overrides
        case Some(mapAnon: Map[_, _]) =>
          val map = mapAnon collect { case (k: String, v: Any) => k -> v }
          require(map("version") == 1)
          map.get("config") match {
            case Some(config: Map[_, _]) =>
              for(configTup <- config)
                configTup match {
                  case (k: String, v: String) =>
                    props.setProperty(k, v)
                  case _ => throw new IllegalArgumentException("Invalid " + entityType + " config: " + str)
                }
            case _ => throw new IllegalArgumentException("Invalid " + entityType + " config: " + str)
          }

        case o => throw new IllegalArgumentException("Unexpected value in config:(%s), entity_type: (%s), entity: (%s)"
                                                             .format(str, entityType, entity))
      }
    }
    props
  }

  def fetchAllTopicConfigs(zkUtils: ZkUtils): Map[String, Properties] =
    zkUtils.getAllTopics().map(topic => (topic, fetchEntityConfig(zkUtils, ConfigType.Topic, topic))).toMap

  def fetchAllEntityConfigs(zkUtils: ZkUtils, entityType: String): Map[String, Properties] =
    zkUtils.getAllEntitiesWithConfig(entityType).map(entity => (entity, fetchEntityConfig(zkUtils, entityType, entity))).toMap

  def fetchTopicMetadataFromZk(topic: String, zkUtils: ZkUtils): MetadataResponse.TopicMetadata =
    fetchTopicMetadataFromZk(topic, zkUtils, new mutable.HashMap[Int, Broker])

  def fetchTopicMetadataFromZk(topics: Set[String], zkUtils: ZkUtils): Set[MetadataResponse.TopicMetadata] = {
    val cachedBrokerInfo = new mutable.HashMap[Int, Broker]()
    topics.map(topic => fetchTopicMetadataFromZk(topic, zkUtils, cachedBrokerInfo))
  }

  private def fetchTopicMetadataFromZk(topic: String,
                                       zkUtils: ZkUtils,
                                       cachedBrokerInfo: mutable.HashMap[Int, Broker],
                                       protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT): MetadataResponse.TopicMetadata = {
    if(zkUtils.pathExists(getTopicPath(topic))) {
      val topicPartitionAssignment = zkUtils.getPartitionAssignmentForTopics(List(topic)).get(topic).get
      val sortedPartitions = topicPartitionAssignment.toList.sortWith((m1, m2) => m1._1 < m2._1)
      val partitionMetadata = sortedPartitions.map { partitionMap =>
        val partition = partitionMap._1
        val replicas = partitionMap._2
        val inSyncReplicas = zkUtils.getInSyncReplicasForPartition(topic, partition)
        val leader = zkUtils.getLeaderForPartition(topic, partition)
        debug("replicas = " + replicas + ", in sync replicas = " + inSyncReplicas + ", leader = " + leader)

        var leaderInfo: Node = Node.noNode()
        var replicaInfo: Seq[Node] = Nil
        var isrInfo: Seq[Node] = Nil
        try {
          leaderInfo = leader match {
            case Some(l) =>
              try {
                getBrokerInfoFromCache(zkUtils, cachedBrokerInfo, List(l)).head.getNode(protocol)
              } catch {
                case e: Throwable => throw new LeaderNotAvailableException("Leader not available for partition [%s,%d]".format(topic, partition), e)
              }
            case None => throw new LeaderNotAvailableException("No leader exists for partition " + partition)
          }
          try {
            replicaInfo = getBrokerInfoFromCache(zkUtils, cachedBrokerInfo, replicas).map(_.getNode(protocol))
            isrInfo = getBrokerInfoFromCache(zkUtils, cachedBrokerInfo, inSyncReplicas).map(_.getNode(protocol))
          } catch {
            case e: Throwable => throw new ReplicaNotAvailableException(e)
          }
          if(replicaInfo.size < replicas.size)
            throw new ReplicaNotAvailableException("Replica information not available for following brokers: " +
              replicas.filterNot(replicaInfo.map(_.id).contains(_)).mkString(","))
          if(isrInfo.size < inSyncReplicas.size)
            throw new ReplicaNotAvailableException("In Sync Replica information not available for following brokers: " +
              inSyncReplicas.filterNot(isrInfo.map(_.id).contains(_)).mkString(","))
          new MetadataResponse.PartitionMetadata(Errors.NONE, partition, leaderInfo, replicaInfo.asJava, isrInfo.asJava)
        } catch {
          case e: Throwable =>
            debug("Error while fetching metadata for partition [%s,%d]".format(topic, partition), e)
            new MetadataResponse.PartitionMetadata(Errors.forException(e), partition, leaderInfo, replicaInfo.asJava, isrInfo.asJava)
        }
      }
      new MetadataResponse.TopicMetadata(Errors.NONE, topic, Topic.isInternal(topic), partitionMetadata.asJava)
    } else {
      // topic doesn't exist, send appropriate error code
      new MetadataResponse.TopicMetadata(Errors.UNKNOWN_TOPIC_OR_PARTITION, topic, Topic.isInternal(topic), java.util.Collections.emptyList())
    }
  }

  private def getBrokerInfoFromCache(zkUtils: ZkUtils,
                                     cachedBrokerInfo: scala.collection.mutable.Map[Int, Broker],
                                     brokerIds: Seq[Int]): Seq[Broker] = {
    var failedBrokerIds: ListBuffer[Int] = new ListBuffer()
    val brokerMetadata = brokerIds.map { id =>
      val optionalBrokerInfo = cachedBrokerInfo.get(id)
      optionalBrokerInfo match {
        case Some(brokerInfo) => Some(brokerInfo) // return broker info from the cache
        case None => // fetch it from zookeeper
          zkUtils.getBrokerInfo(id) match {
            case Some(brokerInfo) =>
              cachedBrokerInfo += (id -> brokerInfo)
              Some(brokerInfo)
            case None =>
              failedBrokerIds += id
              None
          }
      }
    }
    brokerMetadata.filter(_.isDefined).map(_.get)
  }

  // 计算副本索引
  private def replicaIndex(firstReplicaIndex: Int, secondReplicaShift: Int, replicaIndex: Int, nBrokers: Int): Int = {
    val shift = 1 + (secondReplicaShift + replicaIndex) % (nBrokers - 1)
    (firstReplicaIndex + shift) % nBrokers
  }
}
