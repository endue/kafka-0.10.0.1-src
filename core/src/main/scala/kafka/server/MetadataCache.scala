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

package kafka.server

import java.util.EnumMap
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.{Seq, Set, mutable}
import scala.collection.JavaConverters._
import kafka.cluster.{Broker, EndPoint}
import kafka.api._
import kafka.common.{BrokerEndPointNotAvailableException, Topic, TopicAndPartition}
import kafka.controller.{KafkaController, LeaderIsrAndControllerEpoch}
import kafka.utils.CoreUtils._
import kafka.utils.Logging
import org.apache.kafka.common.Node
import org.apache.kafka.common.protocol.{Errors, SecurityProtocol}
import org.apache.kafka.common.requests.{MetadataResponse, PartitionState, UpdateMetadataRequest}

/**
 *  A cache for the state (e.g., current leader) of each partition. This cache is updated through
 *  UpdateMetadataRequest from the controller. Every broker maintains the same cache, asynchronously.
 */
private[server] class MetadataCache(brokerId: Int) extends Logging {
  private val stateChangeLogger = KafkaController.stateChangeLogger
  // key是topic，value中的key是partitionId
  private val cache = mutable.Map[String, mutable.Map[Int, PartitionStateInfo]]()
  // 记录controllerId
  private var controllerId: Option[Int] = None
  // 记录活跃的broker，key是brokerId,value是Broker相关信息
  private val aliveBrokers = mutable.Map[Int, Broker]()
  // 记录当前key的节点
  private val aliveNodes = mutable.Map[Int, collection.Map[SecurityProtocol, Node]]()
  private val partitionMetadataLock = new ReentrantReadWriteLock()

  this.logIdent = s"[Kafka Metadata Cache on broker $brokerId] "

  // This method is the main hotspot when it comes to the performance of metadata requests,
  // we should be careful about adding additional logic here.
  // filterUnavailableEndpoints exists to support v0 MetadataResponses
  private def getEndpoints(brokers: Iterable[Int], protocol: SecurityProtocol, filterUnavailableEndpoints: Boolean): Seq[Node] = {
    val result = new mutable.ArrayBuffer[Node](math.min(aliveBrokers.size, brokers.size))
    brokers.foreach { brokerId =>
      val endpoint = getAliveEndpoint(brokerId, protocol) match {
        case None => if (!filterUnavailableEndpoints) Some(new Node(brokerId, "", -1)) else None
        case Some(node) => Some(node)
      }
      endpoint.foreach(result +=)
    }
    result
  }

  private def getAliveEndpoint(brokerId: Int, protocol: SecurityProtocol): Option[Node] =
    aliveNodes.get(brokerId).map { nodeMap =>
      nodeMap.getOrElse(protocol,
        throw new BrokerEndPointNotAvailableException(s"Broker `$brokerId` does not support security protocol `$protocol`"))
    }

  // errorUnavailableEndpoints exists to support v0 MetadataResponses
  // 获取topic的所有Partition的元数据
  private def getPartitionMetadata(topic: String, protocol: SecurityProtocol, errorUnavailableEndpoints: Boolean): Option[Iterable[MetadataResponse.PartitionMetadata]] = {
    // 从缓存取出topic的所有分区并遍历每一个分区
    cache.get(topic).map { partitions =>
      partitions.map { case (partitionId, partitionState) =>
        // 当前topic-partition
        val topicPartition = TopicAndPartition(topic, partitionId)
        // ISR列表
        val leaderAndIsr = partitionState.leaderIsrAndControllerEpoch.leaderAndIsr
        // 获取leader
        val maybeLeader = getAliveEndpoint(leaderAndIsr.leader, protocol)
        // 获取AR列表
        val replicas = partitionState.allReplicas
        // 获取所有AR列表中服务的Node
        val replicaInfo = getEndpoints(replicas, protocol, errorUnavailableEndpoints)

        maybeLeader match {
          // 没有leader
          case None =>
            debug(s"Error while fetching metadata for $topicPartition: leader not available")
            // 封装分区元数据
            new MetadataResponse.PartitionMetadata(Errors.LEADER_NOT_AVAILABLE, partitionId, Node.noNode(),
              replicaInfo.asJava, java.util.Collections.emptyList())
          // 存在leader
          case Some(leader) =>
            // 获取ISR列表
            val isr = leaderAndIsr.isr
            // 获取ISR列表中节点对应的Node实例
            val isrInfo = getEndpoints(isr, protocol, errorUnavailableEndpoints)

            if (replicaInfo.size < replicas.size) {
              debug(s"Error while fetching metadata for $topicPartition: replica information not available for " +
                s"following brokers ${replicas.filterNot(replicaInfo.map(_.id).contains).mkString(",")}")

              new MetadataResponse.PartitionMetadata(Errors.REPLICA_NOT_AVAILABLE, partitionId, leader,
                replicaInfo.asJava, isrInfo.asJava)
            } else if (isrInfo.size < isr.size) {
              debug(s"Error while fetching metadata for $topicPartition: in sync replica information not available for " +
                s"following brokers ${isr.filterNot(isrInfo.map(_.id).contains).mkString(",")}")
              new MetadataResponse.PartitionMetadata(Errors.REPLICA_NOT_AVAILABLE, partitionId, leader,
                replicaInfo.asJava, isrInfo.asJava)
            } else {
              // 正常返回结果信息
              new MetadataResponse.PartitionMetadata(Errors.NONE, partitionId, leader, replicaInfo.asJava,
                isrInfo.asJava)
            }
        }
      }
    }
  }

  // errorUnavailableEndpoints exists to support v0 MetadataResponses
  // 获取topicds的元数据
  def getTopicMetadata(topics: Set[String], protocol: SecurityProtocol, errorUnavailableEndpoints: Boolean = false): Seq[MetadataResponse.TopicMetadata] = {
    inReadLock(partitionMetadataLock) {
      topics.toSeq.flatMap { topic =>
        // 获取topic的所有分区的元数据，最后封装为topic的元数据
        getPartitionMetadata(topic, protocol, errorUnavailableEndpoints).map { partitionMetadata =>
          new MetadataResponse.TopicMetadata(Errors.NONE, topic, Topic.isInternal(topic), partitionMetadata.toBuffer.asJava)
        }
      }
    }
  }

  def hasTopicMetadata(topic: String): Boolean = {
    inReadLock(partitionMetadataLock) {
      cache.contains(topic)
    }
  }

  def getAllTopics(): Set[String] = {
    inReadLock(partitionMetadataLock) {
      cache.keySet.toSet
    }
  }

  def getNonExistingTopics(topics: Set[String]): Set[String] = {
    inReadLock(partitionMetadataLock) {
      topics -- cache.keySet
    }
  }

  def getAliveBrokers: Seq[Broker] = {
    inReadLock(partitionMetadataLock) {
      aliveBrokers.values.toBuffer
    }
  }

  // 添加或更新对应topic-partition的PartitionStateInfo
  private def addOrUpdatePartitionInfo(topic: String,
                                       partitionId: Int,
                                       stateInfo: PartitionStateInfo) {
    inWriteLock(partitionMetadataLock) {
      val infos = cache.getOrElseUpdate(topic, mutable.Map())
      infos(partitionId) = stateInfo
    }
  }

  def getPartitionInfo(topic: String, partitionId: Int): Option[PartitionStateInfo] = {
    inReadLock(partitionMetadataLock) {
      cache.get(topic).flatMap(_.get(partitionId))
    }
  }

  def getControllerId: Option[Int] = controllerId

  // 更新缓存
  def updateCache(correlationId: Int, updateMetadataRequest: UpdateMetadataRequest) {
    inWriteLock(partitionMetadataLock) {
      controllerId = updateMetadataRequest.controllerId match {
          case id if id < 0 => None
          case id => Some(id)
        }
      // 清空缓存中的aliveNodes和aliveBrokers,准备更新为最新的记录
      aliveNodes.clear()
      aliveBrokers.clear()
      // 更新lieveBrokers
      updateMetadataRequest.liveBrokers.asScala.foreach { broker =>
        val nodes = new EnumMap[SecurityProtocol, Node](classOf[SecurityProtocol])
        val endPoints = new EnumMap[SecurityProtocol, EndPoint](classOf[SecurityProtocol])
        // 遍历endPoints,将相关内容保存到定义的集合中
        broker.endPoints.asScala.foreach { case (protocol, ep) =>
          endPoints.put(protocol, EndPoint(ep.host, ep.port, protocol))
          nodes.put(protocol, new Node(broker.id, ep.host, ep.port))
        }
        aliveBrokers(broker.id) = Broker(broker.id, endPoints.asScala, Option(broker.rack))
        aliveNodes(broker.id) = nodes.asScala
      }
      // 更新本地分区信息
      updateMetadataRequest.partitionStates.asScala.foreach { case (tp, info) =>
        val controllerId = updateMetadataRequest.controllerId
        val controllerEpoch = updateMetadataRequest.controllerEpoch
        // 如果是删除，参考：kafka.controller.ControllerBrokerRequestBatch.addUpdateMetadataRequestForBrokers
        if (info.leader == LeaderAndIsr.LeaderDuringDelete) {
          removePartitionInfo(tp.topic, tp.partition)
          stateChangeLogger.trace(s"Broker $brokerId deleted partition $tp from metadata cache in response to UpdateMetadata " +
            s"request sent by controller $controllerId epoch $controllerEpoch with correlation id $correlationId")
        } else {
          // 解析info返回一个PartitionStateInfo
          val partitionInfo = partitionStateToPartitionStateInfo(info)
          addOrUpdatePartitionInfo(tp.topic, tp.partition, partitionInfo)
          stateChangeLogger.trace(s"Broker $brokerId cached leader info $partitionInfo for partition $tp in response to " +
            s"UpdateMetadata request sent by controller $controllerId epoch $controllerEpoch with correlation id $correlationId")
        }
      }
    }
  }

  private def partitionStateToPartitionStateInfo(partitionState: PartitionState): PartitionStateInfo = {
    val leaderAndIsr = LeaderAndIsr(partitionState.leader, partitionState.leaderEpoch, partitionState.isr.asScala.map(_.toInt).toList, partitionState.zkVersion)
    val leaderInfo = LeaderIsrAndControllerEpoch(leaderAndIsr, partitionState.controllerEpoch)
    PartitionStateInfo(leaderInfo, partitionState.replicas.asScala.map(_.toInt))
  }

  def contains(topic: String): Boolean = {
    inReadLock(partitionMetadataLock) {
      cache.contains(topic)
    }
  }

  // 删除对应的topic-partition
  private def removePartitionInfo(topic: String, partitionId: Int): Boolean = {
    cache.get(topic).map { infos =>
      infos.remove(partitionId)
      if (infos.isEmpty) cache.remove(topic)// 如果分区信息为空，那么删除这个topic
      true
    }.getOrElse(false)
  }

}
