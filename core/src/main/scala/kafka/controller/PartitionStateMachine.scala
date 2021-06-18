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
package kafka.controller

import collection._
import collection.JavaConversions
import collection.mutable.Buffer
import java.util.concurrent.atomic.AtomicBoolean
import kafka.api.LeaderAndIsr
import kafka.common.{LeaderElectionNotNeededException, TopicAndPartition, StateChangeFailedException, NoReplicaOnlineException}
import kafka.utils.{Logging, ReplicationUtils}
import kafka.utils.ZkUtils._
import org.I0Itec.zkclient.{IZkDataListener, IZkChildListener}
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import kafka.controller.Callbacks.CallbackBuilder
import kafka.utils.CoreUtils._

/**
  * ControllerLeader用来管理各个分区的状态，分区状态共分4种：
 * This class represents the state machine for partitions. It defines the states that a partition can be in, and
 * transitions to move the partition to another legal state. The different states that a partition can be in are -
 * 1. NonExistentPartition: This state indicates that the partition was either never created or was created and then
 *                          deleted. Valid previous state, if one exists, is OfflinePartition
  *                          当分区未被创建或者创建后被删除
 * 2. NewPartition        : After creation, the partition is in the NewPartition state. In this state, the partition should have
 *                          replicas assigned to it, but no leader/isr yet. Valid previous states are NonExistentPartition
  *                          分区被创建后，此时该分区已有对应的AR列表，但是还没有LSR列表或者leader
 * 3. OnlinePartition     : Once a leader is elected for a partition, it is in the OnlinePartition state.
 *                          Valid previous states are NewPartition/OfflinePartition
  *                          当分区leader副本被成功选举后
 * 4. OfflinePartition    : If, after successful leader election, the leader for partition dies, then the partition
 *                          moves to the OfflinePartition state. Valid previous states are NewPartition/OnlinePartition
  *                          被选举的leader宕机后
 */
class PartitionStateMachine(controller: KafkaController) extends Logging {
  // 用于维护KafkaController中上下文信息
  private val controllerContext = controller.controllerContext
  // 当前controller的id
  private val controllerId = controller.config.brokerId
  private val zkUtils = controllerContext.zkUtils
  // 记录topic-partiton对应的分区状态，也就是某个主题分区的状态
  private val partitionState: mutable.Map[TopicAndPartition, PartitionState] = mutable.Map.empty
  // 用于向指定的Broker批量发送请求
  private val brokerRequestBatch = new ControllerBrokerRequestBatch(controller)
  // 当前PartitionStateMachine的状态
  private val hasStarted = new AtomicBoolean(false)
  // 默认的副本选举器，并没有真正进行副本选举，只是返回当前的Leader副本，ISR集合和AR集合
  private val noOpPartitionLeaderSelector = new NoOpLeaderSelector(controllerContext)
  // 监听topic的变化,路径为：/brokers/topics
  private val topicChangeListener = new TopicChangeListener()
  // 监听topic的删除,路径为：/admin/delete_topics
  private val deleteTopicsListener = new DeleteTopicsListener()
  // 监听分区的修改，key是topic，路径为:/brokers/topics/{topic}
  private val partitionModificationsListeners: mutable.Map[String, PartitionModificationsListener] = mutable.Map.empty
  private val stateChangeLogger = KafkaController.stateChangeLogger

  this.logIdent = "[Partition state machine on Controller " + controllerId + "]: "

  /**
   * Invoked on successful controller election. First registers a topic change listener since that triggers all
   * state transitions for partitions. Initializes the state of partitions by reading from zookeeper. Then triggers
   * the OnlinePartition state change for all new or offline partitions.
    * 启动state machine
    * 初始化所有分区的状态（从ZK获取）, 然后对于 New/Offline 触发选主（选主成功后, 变为 OnlinePartition）
   */
  def startup() {
    // initialize partition state 初始化各个分区状态
    // 如果没有对应的信息那么设置状态为NewPartition
    // 如果leader所在的broker是存活的设置状态为OnlinePartition，否则为OfflinePartition
    initializePartitionState()
    // set started flag 设置启动标识
    hasStarted.set(true)
    // try to move partitions to online state
    // 尝试将分区移动到online状态
    triggerOnlinePartitionStateChange()

    info("Started partition state machine with initial state -> " + partitionState.toString())
  }

  // register topic and partition change listeners
  /**
    * 当前kafka服务启动选举成为broker leader后执行回调方法onControllerFailover()
    * 调用partitionStateMachine.registerListeners()方法
    */
  def registerListeners() {
    registerTopicChangeListener()
    // 是否配置“delete.topic.enable”为true
    if(controller.config.deleteTopicEnable)
      registerDeleteTopicListener()
  }

  // de-register topic and partition change listeners
  // 取消注册主题和分区更改侦听器
  def deregisterListeners() {
    deregisterTopicChangeListener()
    partitionModificationsListeners.foreach {
      case (topic, listener) =>
        zkUtils.zkClient.unsubscribeDataChanges(getTopicPath(topic), listener)
    }
    partitionModificationsListeners.clear()
    if(controller.config.deleteTopicEnable)
      deregisterDeleteTopicListener()
  }

  /**
   * Invoked on controller shutdown.
   */
  def shutdown() {
    // reset started flag
    hasStarted.set(false)
    // clear partition state
    partitionState.clear()
    // de-register all ZK listeners
    deregisterListeners()

    info("Stopped partition state machine")
  }

  /**
   * This API invokes the OnlinePartition state change on all partitions in either the NewPartition or OfflinePartition
   * state. This is called on a successful controller election and on broker changes
    * 首先该方法在kafka controller被选举后调用，在broker发生变更后调用
    * 功能是将NewPartition 或 OfflinePartition状态的分区转换为OnlinePartition
   */
  def triggerOnlinePartitionStateChange() {
    try {
      // 在发送请求前,检查是否有未发生的请求
      brokerRequestBatch.newBatch()
      // try to move all partitions in NewPartition or OfflinePartition state to OnlinePartition state except partitions
      // that belong to topics to be deleted
      // 尝试将所有处于NewPartition或OfflinePartition状态的分区移动到OnlinePartition状态，但属于要删除主题的分区除外

      // 遍历记录分区状态的集合
      for((topicAndPartition, partitionState) <- partitionState
          // 过滤掉将要被删除的分区
          if(!controller.deleteTopicManager.isTopicQueuedUpForDeletion(topicAndPartition.topic))) {
        // 分区状态如果是OfflinePartition 或 NewPartition
        if(partitionState.equals(OfflinePartition) || partitionState.equals(NewPartition))
          // 尝试转换分区状态
          handleStateChange(topicAndPartition.topic, topicAndPartition.partition, OnlinePartition, controller.offlinePartitionSelector,
                            (new CallbackBuilder).build)
      }
      // 发送请求到其他broker上
      brokerRequestBatch.sendRequestsToBrokers(controller.epoch)
    } catch {
      case e: Throwable => error("Error while moving some partitions to the online state", e)
      // TODO: It is not enough to bail out and log an error, it is important to trigger leader election for those partitions
    }
  }

  def partitionsInState(state: PartitionState): Set[TopicAndPartition] = {
    partitionState.filter(p => p._2 == state).keySet
  }

  /**
    * 处理TopicAndPartition状态
   * This API is invoked by the partition change zookeeper listener
   * @param partitions   The list of partitions that need to be transitioned to the target state
    *                     需要转换到目标状态的分区列表
   * @param targetState  The state that the partitions should be moved to
    *                     分区应该移动到的状态
   */
  def handleStateChanges(partitions: Set[TopicAndPartition], targetState: PartitionState,
                         leaderSelector: PartitionLeaderSelector = noOpPartitionLeaderSelector,
                         callbacks: Callbacks = (new CallbackBuilder).build) {
    info("Invoking state change to %s for partitions %s".format(targetState, partitions.mkString(",")))
    try {
      // 校验缓存中是否有未发生的消息
      brokerRequestBatch.newBatch()
      // 遍历topicAndPartition集合
      partitions.foreach { topicAndPartition =>
        handleStateChange(topicAndPartition.topic, topicAndPartition.partition, targetState, leaderSelector, callbacks)
      }
      // 发生请求给集群中其他的broker节点
      brokerRequestBatch.sendRequestsToBrokers(controller.epoch)
    }catch {
      case e: Throwable => error("Error while moving some partitions to %s state".format(targetState), e)
      // TODO: It is not enough to bail out and log an error, it is important to trigger state changes for those partitions
    }
  }

  /**
   * This API exercises the partition's state machine. It ensures that every state transition happens from a legal
   * previous state to the target state. Valid state transitions are:
   * NonExistentPartition -> NewPartition:
   * --load assigned replicas from ZK to controller cache
   *
   * NewPartition -> OnlinePartition
   * --assign first live replica as the leader and all live replicas as the isr; write leader and isr to ZK for this partition
   * --send LeaderAndIsr request to every live replica and UpdateMetadata request to every live broker
   *
   * OnlinePartition,OfflinePartition -> OnlinePartition
   * --select new leader and isr for this partition and a set of replicas to receive the LeaderAndIsr request, and write leader and isr to ZK
   * --for this partition, send LeaderAndIsr request to every receiving replica and UpdateMetadata request to every live broker
   *
   * NewPartition,OnlinePartition,OfflinePartition -> OfflinePartition
   * --nothing other than marking partition state as Offline
   *
   * OfflinePartition -> NonExistentPartition
   * --nothing other than marking the partition state as NonExistentPartition
   * @param topic       The topic of the partition for which the state transition is invoked
   * @param partition   The partition for which the state transition is invoked
   * @param targetState The end state that the partition should be moved to
    * 分区状态机改变处理方法, 可以处理多个分区的状态转换, 这样就可以采用批量方式发送请求给多个Broker
    * 用partitionLeaderElectionStrategyOpt指定的策略去选举Leader
   */
  private def handleStateChange(topic: String, partition: Int, targetState: PartitionState,
                                leaderSelector: PartitionLeaderSelector,
                                callbacks: Callbacks) {
    val topicAndPartition = TopicAndPartition(topic, partition)
    if (!hasStarted.get)
      throw new StateChangeFailedException(("Controller %d epoch %d initiated state change for partition %s to %s failed because " +
                                            "the partition state machine has not started")
                                              .format(controllerId, controller.epoch, topicAndPartition, targetState))
    // 获取当前分区的状态，如果不存在则默认为NonExistentPartition
    val currState = partitionState.getOrElseUpdate(topicAndPartition, NonExistentPartition)
    try {
      // 判断目标状态
      targetState match {
          // 目标状态是NewPartition
        case NewPartition =>
          // pre: partition did not exist before this
          // 验证分区状态
          assertValidPreviousStates(topicAndPartition, List(NonExistentPartition), NewPartition)
          // 修改分区状态
          partitionState.put(topicAndPartition, NewPartition)
          val assignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition).mkString(",")
          stateChangeLogger.trace("Controller %d epoch %d changed partition %s state from %s to %s with assigned replicas %s"
                                    .format(controllerId, controller.epoch, topicAndPartition, currState, targetState,
                                            assignedReplicas))
          // post: partition has been assigned replicas
        // 目标状态是OnlinePartition
        case OnlinePartition =>
          // 验证分区状态
          assertValidPreviousStates(topicAndPartition, List(NewPartition, OnlinePartition, OfflinePartition), OnlinePartition)
          // 变更分区状态前获取分区状态
          partitionState(topicAndPartition) match {
              // 当前分区状态为NewPartition
            case NewPartition =>
              // initialize leader and isr path for new partition
              // 为当前分区初始化leader和isr
              initializeLeaderAndIsrForPartition(topicAndPartition)
            // 当前分区状态为OfflinePartition
            case OfflinePartition =>
              // 为当前分区重新选举
              electLeaderForPartition(topic, partition, leaderSelector)
            // 当前分区状态为OnlinePartition，因为某种原因重新选举，参考kafka.controller.KafkaController.checkAndTriggerPartitionRebalance
            case OnlinePartition => // invoked when the leader needs to be re-elected
              // 为当前分区重新选举
              electLeaderForPartition(topic, partition, leaderSelector)
            case _ => // should never come here since illegal previous states are checked above
          }
          // 修改分支状态
          partitionState.put(topicAndPartition, OnlinePartition)
          val leader = controllerContext.partitionLeadershipInfo(topicAndPartition).leaderAndIsr.leader
          stateChangeLogger.trace("Controller %d epoch %d changed partition %s from %s to %s with leader %d"
                                    .format(controllerId, controller.epoch, topicAndPartition, currState, targetState, leader))
           // post: partition has a leader
        // 目标状态是OfflinePartition
        case OfflinePartition =>
          // pre: partition should be in New or Online state
          // 验证分区状态
          assertValidPreviousStates(topicAndPartition, List(NewPartition, OnlinePartition, OfflinePartition), OfflinePartition)
          // should be called when the leader for a partition is no longer alive
          stateChangeLogger.trace("Controller %d epoch %d changed partition %s state from %s to %s"
                                    .format(controllerId, controller.epoch, topicAndPartition, currState, targetState))
          // 修改分支状态
          partitionState.put(topicAndPartition, OfflinePartition)
          // post: partition has no alive leader
        // 目标状态是NonExistentPartition
        case NonExistentPartition =>
          // pre: partition should be in Offline state
          // 验证分区状态
          assertValidPreviousStates(topicAndPartition, List(OfflinePartition), NonExistentPartition)
          stateChangeLogger.trace("Controller %d epoch %d changed partition %s state from %s to %s"
                                    .format(controllerId, controller.epoch, topicAndPartition, currState, targetState))
          // 修改分支状态
          partitionState.put(topicAndPartition, NonExistentPartition)
          // post: partition state is deleted from all brokers and zookeeper
      }
    } catch {
      case t: Throwable =>
        stateChangeLogger.error("Controller %d epoch %d initiated state change for partition %s from %s to %s failed"
          .format(controllerId, controller.epoch, topicAndPartition, currState, targetState), t)
    }
  }

  /**
   * Invoked on startup of the partition's state machine to set the initial state for all existing partitions in
   * zookeeper
    * 当state machine启动后，为zookeeper中所有存在的分区设置初始状态
   */
  private def initializePartitionState() {
    // 遍历每一个topic-partiton
    for((topicPartition, replicaAssignment) <- controllerContext.partitionReplicaAssignment) {
      // check if leader and isr path exists for partition. If not, then it is in NEW state
      // 检查主题分区是否存在LeaderIsrAndControllerEpoch信息。如果没有则它处于NEW状态
      controllerContext.partitionLeadershipInfo.get(topicPartition) match {
        // 存在，说明是已存在的主题分区
        case Some(currentLeaderIsrAndEpoch) =>
          // else, check if the leader for partition is alive. If yes, it is in Online state, else it is in Offline state
          // 检查当前主题分区的leader是否为live
          controllerContext.liveBrokerIds.contains(currentLeaderIsrAndEpoch.leaderAndIsr.leader) match {
            case true => // leader is alive
              // leader为live将主题分区状态初始化为OnlinePartition
              partitionState.put(topicPartition, OnlinePartition)
            case false =>
              // leader不为live将主题分区状态初始化为OfflinePartition
              partitionState.put(topicPartition, OfflinePartition)
          }
        // 不存在，说明是新建的主题分区，初始化状态为NewPartition
        case None =>
          partitionState.put(topicPartition, NewPartition)
      }
    }
  }

  /**
    * 验证主题分区状态
    * @param topicAndPartition 主题分区
    * @param fromStates 主题分区原状态
    * @param targetState 主题分区目标状态
    */
  private def assertValidPreviousStates(topicAndPartition: TopicAndPartition, fromStates: Seq[PartitionState],
                                        targetState: PartitionState) {
    // 如果原状态不包括当前主题分区状态抛出异常
    if(!fromStates.contains(partitionState(topicAndPartition)))
      throw new IllegalStateException("Partition %s should be in the %s states before moving to %s state"
        .format(topicAndPartition, fromStates.mkString(","), targetState) + ". Instead it is in %s state"
        .format(partitionState(topicAndPartition)))
  }

  /**
   * Invoked on the NewPartition->OnlinePartition state change. When a partition is in the New state, it does not have
   * a leader and isr path in zookeeper. Once the partition moves to the OnlinePartition state, its leader and isr
   * path gets initialized and it never goes back to the NewPartition state. From here, it can only go to the
   * OfflinePartition state.
    * 主题分区状态从NewPartition->OnlinePartition变化，如果一个主题分区状态被置为OnlinePartition那么它永远不会回到NewPartition
    * 它只能转变为OfflinePartition
   * @param topicAndPartition   The topic/partition whose leader and isr path is to be initialized
   */
  private def initializeLeaderAndIsrForPartition(topicAndPartition: TopicAndPartition) {
    // 获取当前主题分区的ar副本集合
    val replicaAssignment = controllerContext.partitionReplicaAssignment(topicAndPartition)
    // 过滤出当前主题分区live的副本集合
    val liveAssignedReplicas = replicaAssignment.filter(r => controllerContext.liveBrokerIds.contains(r))
    // 判断live副本的数量
    liveAssignedReplicas.size match {
      // live副本数量为0
      case 0 =>
        // live副本数量为抛出StateChangeFailedException异常
        val failMsg = ("encountered error during state change of partition %s from New to Online, assigned replicas are [%s], " +
                       "live brokers are [%s]. No assigned replica is alive.")
                         .format(topicAndPartition, replicaAssignment.mkString(","), controllerContext.liveBrokerIds)
        stateChangeLogger.error("Controller %d epoch %d ".format(controllerId, controller.epoch) + failMsg)
        throw new StateChangeFailedException(failMsg)
      // live副本数量非0
      case _ =>
        debug("Live assigned replicas for partition %s are: [%s]".format(topicAndPartition, liveAssignedReplicas))
        // make the first replica in the list of assigned replicas, the leader
        // live的副本集合的第一个副本作为leader
        val leader = liveAssignedReplicas.head
        // 构建LeaderIsrAndControllerEpoch
        val leaderIsrAndControllerEpoch = new LeaderIsrAndControllerEpoch(new LeaderAndIsr(leader, liveAssignedReplicas.toList),
          controller.epoch)
        debug("Initializing leader and isr for partition %s to %s".format(topicAndPartition, leaderIsrAndControllerEpoch))
        try {
          // 创建持久节点,两个参数:路径，数据
          zkUtils.createPersistentPath(
            // 路径：/brokers/topics/{topic}/partitions/{partitionId}/state
            getTopicPartitionLeaderAndIsrPath(topicAndPartition.topic, topicAndPartition.partition),
            // 数据：
            // Json.encode(Map("version" -> 1, "leader" -> leaderAndIsr.leader, "leader_epoch" -> leaderAndIsr.leaderEpoch,"controller_epoch" -> controllerEpoch, "isr" -> leaderAndIsr.isr))
            // {"controller_epoch":14,"leader":1,"version":1,"leader_epoch":23,"isr":[1,2]}
            zkUtils.leaderAndIsrZkData(leaderIsrAndControllerEpoch.leaderAndIsr, controller.epoch))
          // NOTE: the above write can fail only if the current controller lost its zk session and the new controller
          // took over and initialized this partition. This can happen if the current controller went into a long
          // GC pause
          // 更新controllerContext中partitionLeadershipInfo记录的主题分区和对应的LeaderIsrAndControllerEpoch信息
          controllerContext.partitionLeadershipInfo.put(topicAndPartition, leaderIsrAndControllerEpoch)
          // 构建leaderAndIsrRequest发送到ar列表中有效的broker节点
          brokerRequestBatch.addLeaderAndIsrRequestForBrokers(liveAssignedReplicas, topicAndPartition.topic,
            topicAndPartition.partition, leaderIsrAndControllerEpoch, replicaAssignment)
        } catch {
          case e: ZkNodeExistsException =>
            // read the controller epoch
            val leaderIsrAndEpoch = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, topicAndPartition.topic,
              topicAndPartition.partition).get
            val failMsg = ("encountered error while changing partition %s's state from New to Online since LeaderAndIsr path already " +
                           "exists with value %s and controller epoch %d")
                             .format(topicAndPartition, leaderIsrAndEpoch.leaderAndIsr.toString(), leaderIsrAndEpoch.controllerEpoch)
            stateChangeLogger.error("Controller %d epoch %d ".format(controllerId, controller.epoch) + failMsg)
            throw new StateChangeFailedException(failMsg)
        }
    }
  }

  /**
   * Invoked on the OfflinePartition,OnlinePartition->OnlinePartition state change.
   * It invokes the leader election API to elect a leader for the input offline partition
   * @param topic               The topic of the offline partition
   * @param partition           The offline partition
   * @param leaderSelector      Specific leader selector (e.g., offline/reassigned/etc.)
    * 处理OfflinePartition或OnlinePartition->OnlinePartition变化，为分区选择leader
   */
  def electLeaderForPartition(topic: String, partition: Int, leaderSelector: PartitionLeaderSelector) {
    // 获取主题分区
    val topicAndPartition = TopicAndPartition(topic, partition)
    // handle leader election for the partitions whose leader is no longer alive
    stateChangeLogger.trace("Controller %d epoch %d started leader election for partition %s"
                              .format(controllerId, controller.epoch, topicAndPartition))
    try {
      var zookeeperPathUpdateSucceeded: Boolean = false
      var newLeaderAndIsr: LeaderAndIsr = null
      var replicasForThisPartition: Seq[Int] = Seq.empty[Int]
      while(!zookeeperPathUpdateSucceeded) {
        // 获取当前主题分区的LeaderIsrAndControllerEpoch信息，不存在会抛出StateChangeFailedException
        val currentLeaderIsrAndEpoch = getLeaderIsrAndEpochOrThrowException(topic, partition)
        // 获取当前主题分区的LeaderAndIsr
        val currentLeaderAndIsr = currentLeaderIsrAndEpoch.leaderAndIsr
        // 获取当前主题分区所处的controllerEpoch
        val controllerEpoch = currentLeaderIsrAndEpoch.controllerEpoch
        // 如果当前主题分区所处的controllerEpoch > 当前controller.epoch，抛出StateChangeFailedException
        if (controllerEpoch > controller.epoch) {
          val failMsg = ("aborted leader election for partition [%s,%d] since the LeaderAndIsr path was " +
                         "already written by another controller. This probably means that the current controller %d went through " +
                         "a soft failure and another controller was elected with epoch %d.")
                           .format(topic, partition, controllerId, controllerEpoch)
          stateChangeLogger.error("Controller %d epoch %d ".format(controllerId, controller.epoch) + failMsg)
          throw new StateChangeFailedException(failMsg)
        }
        // elect new leader or throw exception
        // 选择新leader和isr列表
        val (leaderAndIsr, replicas) = leaderSelector.selectLeader(topicAndPartition, currentLeaderAndIsr)
        // 更新zk中记录的信息，路径：/brokers/topics/{topic}/partitions/{partitionId}/state
        val (updateSucceeded, newVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, topic, partition,
          leaderAndIsr, controller.epoch, currentLeaderAndIsr.zkVersion)
        newLeaderAndIsr = leaderAndIsr
        newLeaderAndIsr.zkVersion = newVersion
        zookeeperPathUpdateSucceeded = updateSucceeded
        replicasForThisPartition = replicas
      }
      val newLeaderIsrAndControllerEpoch = new LeaderIsrAndControllerEpoch(newLeaderAndIsr, controller.epoch)
      // update the leader cache
      // 更新controllerContext中partitionLeadershipInfo记录的分区和对应的LeaderIsrAndControllerEpoch信息
      controllerContext.partitionLeadershipInfo.put(TopicAndPartition(topic, partition), newLeaderIsrAndControllerEpoch)
      stateChangeLogger.trace("Controller %d epoch %d elected leader %d for Offline partition %s"
                                .format(controllerId, controller.epoch, newLeaderAndIsr.leader, topicAndPartition))
      // 获取当前主题分区ar列表
      val replicas = controllerContext.partitionReplicaAssignment(TopicAndPartition(topic, partition))
      // store new leader and isr info in cache
      // 构建leaderAndIsrRequest等待发送到其他broker
      brokerRequestBatch.addLeaderAndIsrRequestForBrokers(replicasForThisPartition, topic, partition,
        newLeaderIsrAndControllerEpoch, replicas)
    } catch {
      case lenne: LeaderElectionNotNeededException => // swallow
      case nroe: NoReplicaOnlineException => throw nroe
      case sce: Throwable =>
        val failMsg = "encountered error while electing leader for partition %s due to: %s.".format(topicAndPartition, sce.getMessage)
        stateChangeLogger.error("Controller %d epoch %d ".format(controllerId, controller.epoch) + failMsg)
        throw new StateChangeFailedException(failMsg, sce)
    }
    debug("After leader election, leader cache is updated to %s".format(controllerContext.partitionLeadershipInfo.map(l => (l._1, l._2))))
  }

  /**
    * 当前kafka服务启动选举成为broker leader后执行回调方法onControllerFailover()调用partitionStateMachine.registerListeners()方法
    * 注册"/brokers/topics"节点下的topicChangeListener
    * @return
    */
  private def registerTopicChangeListener() = {

    zkUtils.zkClient.subscribeChildChanges(BrokerTopicsPath, topicChangeListener)
  }

  // 当退位controller leader后执行
  // 路径：/brokers/topics
  private def deregisterTopicChangeListener() = {
    zkUtils.zkClient.unsubscribeChildChanges(BrokerTopicsPath, topicChangeListener)
  }

  /**
    * 1.当前kafka服务启动选举成为broker leader后执行回调方法onControllerFailover()调用
    *   controllerContext.allTopics.foreach(topic => partitionStateMachine.registerPartitionChangeListener(topic))
    * 2. 当新的topic创建后,调用该方法
    *   kafka.controller.KafkaController#onNewTopicCreation(scala.collection.Set, scala.collection.Set)
    * 注册"/brokers/topics/{topic}"节点下的PartitionModificationsListener
    * @param topic
    */
  def registerPartitionChangeListener(topic: String) = {
    partitionModificationsListeners.put(topic, new PartitionModificationsListener(topic))
    zkUtils.zkClient.subscribeDataChanges(getTopicPath(topic), partitionModificationsListeners(topic))
  }

  // 当退位controller leader后执行
  // 路径：/brokers/topics/{topic}
  def deregisterPartitionChangeListener(topic: String) = {
    zkUtils.zkClient.unsubscribeDataChanges(getTopicPath(topic), partitionModificationsListeners(topic))
    partitionModificationsListeners.remove(topic)
  }

  /**
    * 当前kafka服务启动选举成为broker leader后执行回调方法onControllerFailover()调用partitionStateMachine.registerListeners()方法
    * 注册"/admin/delete_topics"节点下的deleteTopicsListener
    * @return
    */
  private def registerDeleteTopicListener() = {
    zkUtils.zkClient.subscribeChildChanges(DeleteTopicsPath, deleteTopicsListener)
  }

  // 当退位controller leader后执行
  // 路径：/admin/delete_topics
  private def deregisterDeleteTopicListener() = {
    zkUtils.zkClient.unsubscribeChildChanges(DeleteTopicsPath, deleteTopicsListener)
  }

  private def getLeaderIsrAndEpochOrThrowException(topic: String, partition: Int): LeaderIsrAndControllerEpoch = {
    val topicAndPartition = TopicAndPartition(topic, partition)
    ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, topic, partition) match {
      case Some(currentLeaderIsrAndEpoch) => currentLeaderIsrAndEpoch
      case None =>
        val failMsg = "LeaderAndIsr information doesn't exist for partition %s in %s state"
                        .format(topicAndPartition, partitionState(topicAndPartition))
        throw new StateChangeFailedException(failMsg)
    }
  }

  /**
   * This is the zookeeper listener that triggers all the state transitions for a partition
    * 监听的是：/brokers/topics
   */
  class TopicChangeListener extends IZkChildListener with Logging {
    this.logIdent = "[TopicChangeListener on Controller " + controller.config.brokerId + "]: "

    /**
      * @param parentPath 路径
      * @param children 所有的topic
      * @throws java.lang.Exception
      */
    @throws(classOf[Exception])
    def handleChildChange(parentPath : String, children : java.util.List[String]) {
      inLock(controllerContext.controllerLock) {
        // 校验PartitionStateMachine的状态是否还在运行,从而能够判断当前broker是否为leader
        if (hasStarted.get) {
          try {
            // 获取/brokers/topics下的子节点集合
            // 也就是获取所有topic
            val currentChildren = {
              import JavaConversions._
              debug("Topic change listener fired for path %s with children %s".format(parentPath, children.mkString(",")))
              (children: Buffer[String]).toSet
            }
            // 过滤出新增的topics
            val newTopics = currentChildren -- controllerContext.allTopics
            // 过滤出已删除的topics
            val deletedTopics = controllerContext.allTopics -- currentChildren
            // 更新上下文中记录的所有topic
            controllerContext.allTopics = currentChildren
            // 获取新topics的分区分配结果,返回类型Map[TopicAndPartition, Seq[Int]]
            // key是topic-partiton对象，value是副本id
            val addedPartitionReplicaAssignment = zkUtils.getReplicaAssignmentForTopics(newTopics.toSeq)
            // 从controllerContext.partitionReplicaAssignment中过滤掉deletedTopics队列里的topic
            // 也就是更新上下文中topic的ar集合,有的topic已经被删除了,不需要在记录了
            controllerContext.partitionReplicaAssignment = controllerContext.partitionReplicaAssignment.filter(p => !deletedTopics.contains(p._1.topic))
            // 将新的topics累加到controllerContext.partitionReplicaAssignment的ar副本集
            controllerContext.partitionReplicaAssignment.++=(addedPartitionReplicaAssignment)
            info("New topics: [%s], deleted topics: [%s], new partition replica assignment [%s]".format(newTopics,
              deletedTopics, addedPartitionReplicaAssignment))
            // 如果新topics的数量 > 0
            if(newTopics.size > 0)
              // 处理新增的topic
              // 1.增加监听器
              // 2.处理分区和分区副本的状态
              // 3.发生请求到其他broker
              controller.onNewTopicCreation(newTopics, addedPartitionReplicaAssignment.keySet.toSet)
          } catch {
            case e: Throwable => error("Error while handling new topic", e )
          }
        }
      }
    }
  }

  /**
   * Delete topics includes the following operations -
    * 删除topic包括以下操作
   * 1. Add the topic to be deleted to the delete topics cache, only if the topic exists
   * 2. If there are topics to be deleted, it signals the delete topic thread
   */
  class DeleteTopicsListener() extends IZkChildListener with Logging {
    this.logIdent = "[DeleteTopicsListener on " + controller.config.brokerId + "]: "
    val zkUtils = controllerContext.zkUtils

    /**
     * Invoked when a topic is being deleted
      * 当有topic被删除时执行如下方法
     * @throws Exception On any error.
     */
    @throws(classOf[Exception])
    def handleChildChange(parentPath : String, children : java.util.List[String]) {
      inLock(controllerContext.controllerLock) {
        // 获取将被删除的topic集合
        var topicsToBeDeleted = {
          import JavaConversions._
          (children: Buffer[String]).toSet
        }
        debug("Delete topics listener fired for topics %s to be deleted".format(topicsToBeDeleted.mkString(",")))
        // 从topicsToBeDeleted集合过滤出controllerContext.allTopics不包含的topic
        val nonExistentTopics = topicsToBeDeleted.filter(t => !controllerContext.allTopics.contains(t))
        // 如果缓存中没有对应的topic,那么直接删除“/admin/delete_topics/{topic}”节点即可
        if(nonExistentTopics.size > 0) {
          warn("Ignoring request to delete non-existing topics " + nonExistentTopics.mkString(","))
          // 删除对应topic的zk路径
          nonExistentTopics.foreach(topic => zkUtils.deletePathRecursive(getDeleteTopicPath(topic)))
        }
        // 过滤掉不存在的topic，计算剩余存在的topic
        topicsToBeDeleted --= nonExistentTopics
        if(topicsToBeDeleted.size > 0) {
          info("Starting topic deletion for topics " + topicsToBeDeleted.mkString(","))
          // mark topic ineligible for deletion if other state changes are in progress
          // 如果正在进行其他状态更改，则标记topic不适合删除
          topicsToBeDeleted.foreach { topic =>
            // 正在leader副本优化的topic集合
            val preferredReplicaElectionInProgress =
              controllerContext.partitionsUndergoingPreferredReplicaElection.map(_.topic).contains(topic)
            // 正在分区重分配的topic集合
            val partitionReassignmentInProgress =
              controllerContext.partitionsBeingReassigned.keySet.map(_.topic).contains(topic)
            // 查询topic是否正在执行leader副本优化或分区重分配，若是，则标记为暂时不适合被删除
            if(preferredReplicaElectionInProgress || partitionReassignmentInProgress)
              controller.deleteTopicManager.markTopicIneligibleForDeletion(Set(topic))
          }
          // add topic to deletion list
          // 添加topic到待删除的queue中
          controller.deleteTopicManager.enqueueTopicsForDeletion(topicsToBeDeleted)
        }
      }
    }

    /**
     *
     * @throws Exception
   *             On any error.
     */
    @throws(classOf[Exception])
    def handleDataDeleted(dataPath: String) {
    }
  }

  /**
    * 当新建topic时会为当前topic注册一个PartitionModificationsListener
    * 删除topic时会取消PartitionModificationsListener
    * 监听的节点是：/brokers/topics/{topic}
    * 负责topic分区的变化
    * @param topic
    */
  class PartitionModificationsListener(topic: String) extends IZkDataListener with Logging {

    this.logIdent = "[AddPartitionsListener on " + controller.config.brokerId + "]: "

    /**
      * @param dataPath
      * @param data 变更的主题
      * @throws java.lang.Exception
      */
    @throws(classOf[Exception])
    def handleDataChange(dataPath : String, data: Object) {
      inLock(controllerContext.controllerLock) {
        try {
          info(s"Partition modification triggered $data for path $dataPath")
          // 获取topic的ar集合列表
          val partitionReplicaAssignment = zkUtils.getReplicaAssignmentForTopics(List(topic))
          // 过滤出新增加的partition
          val partitionsToBeAdded = partitionReplicaAssignment.filter(p =>
            !controllerContext.partitionReplicaAssignment.contains(p._1))
          // 如果当前topic是待删除topic则打印日志，否则继续处理
          if(controller.deleteTopicManager.isTopicQueuedUpForDeletion(topic))
            error("Skipping adding partitions %s for topic %s since it is currently being deleted"
                  .format(partitionsToBeAdded.map(_._1.partition).mkString(","), topic))
          else {
            // 新增partiton
            if (partitionsToBeAdded.size > 0) {
              info("New partitions to be added %s".format(partitionsToBeAdded))
              // 增加topic的ar副本
              controllerContext.partitionReplicaAssignment.++=(partitionsToBeAdded)
              // 由于是新增partiton，此时触发NewPartitionCreation
              // 1. 变更新增分区状态NewPartition -> OnlinePartition并选择对应的leader，最终上线对外提供服务
              // 2. 变更新增分区副本状态NewReplica -> OnlineReplica
              controller.onNewPartitionCreation(partitionsToBeAdded.keySet.toSet)
            }
          }
        } catch {
          case e: Throwable => error("Error while handling add partitions for data path " + dataPath, e )
        }
      }
    }

    @throws(classOf[Exception])
    def handleDataDeleted(parentPath : String) {
      // this is not implemented for partition change
    }
  }
}

sealed trait PartitionState { def state: Byte }
case object NewPartition extends PartitionState { val state: Byte = 0 }
case object OnlinePartition extends PartitionState { val state: Byte = 1 }
case object OfflinePartition extends PartitionState { val state: Byte = 2 }
case object NonExistentPartition extends PartitionState { val state: Byte = 3 }
