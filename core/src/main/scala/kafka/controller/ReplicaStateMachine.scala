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
import collection.JavaConversions._
import java.util.concurrent.atomic.AtomicBoolean
import kafka.common.{TopicAndPartition, StateChangeFailedException}
import kafka.utils.{ZkUtils, ReplicationUtils, Logging}
import org.I0Itec.zkclient.IZkChildListener
import org.apache.log4j.Logger
import kafka.controller.Callbacks._
import kafka.utils.CoreUtils._

/**
 * This class represents the state machine for replicas. It defines the states that a replica can be in, and
 * transitions to move the replica to another legal state. The different states that a replica can be in are -
  * ReplicaStateMachine是KafkaController维护副本的状态机，副本状态由ReplicaState表示
  *
 * 1. NewReplica        : The controller can create new replicas during partition reassignment. In this state, a
 *                        replica can only get become follower state change request.  Valid previous
 *                        state is NonExistentReplica
  *                        新创建的topic或者topic进行副本重分配的时候，处于这个状态
  *
 * 2. OnlineReplica     : Once a replica is started and part of the assigned replicas for its partition, it is in this
 *                        state. In this state, it can get either become leader or become follower state change requests.
 *                        Valid previous state are NewReplica, OnlineReplica or OfflineReplica
  *                       一旦启动了一个副本，它就成为分区中分配的副本的一部分，在这个状态它可以成为leader或者follower
  *
 * 3. OfflineReplica    : If a replica dies, it moves to this state. This happens when the broker hosting the replica
 *                        is down. Valid previous state are NewReplica, OnlineReplica
  *                        副本宕机
  *
 * 4. ReplicaDeletionStarted: If replica deletion starts, it is moved to this state. Valid previous state is OfflineReplica
  *                        刚开始删除副本，为此状态
  *
 * 5. ReplicaDeletionSuccessful: If replica responds with no error code in response to a delete replica request, it is
 *                        moved to this state. Valid previous state is ReplicaDeletionStarted
  *                        副本删除成功
  *
 * 6. ReplicaDeletionIneligible: If replica deletion fails, it is moved to this state. Valid previous state is ReplicaDeletionStarted
  *                       副本删除失败
  *
 * 7. NonExistentReplica: If a replica is deleted successfully, it is moved to this state. Valid previous state is
 *                        ReplicaDeletionSuccessful
  *                        没有副本或者所有副本被删除
 */
class ReplicaStateMachine(controller: KafkaController) extends Logging {
  // 用于维护KafkaController中上下文信息
  private val controllerContext = controller.controllerContext
  // 当前controller的id
  private val controllerId = controller.config.brokerId
  private val zkUtils = controllerContext.zkUtils
  // 记录partition-replica对应的副本状态，也就是某个分区副本的状态
  private val replicaState: mutable.Map[PartitionAndReplica, ReplicaState] = mutable.Map.empty
  // 监听broker的变化,路径"/brokers/ids"
  private val brokerChangeListener = new BrokerChangeListener()
  // 用于向指定的Broker批量发送请求
  private val brokerRequestBatch = new ControllerBrokerRequestBatch(controller)
  // 当前state machine状态
  private val hasStarted = new AtomicBoolean(false)
  private val stateChangeLogger = KafkaController.stateChangeLogger

  this.logIdent = "[Replica state machine on controller " + controller.config.brokerId + "]: "


  /**
   * Invoked on successful controller election. First registers a broker change listener since that triggers all
   * state transitions for replicas. Initializes the state of replicas for all partitions by reading from zookeeper.
   * Then triggers the OnlineReplica state change for all replicas.
    * 启动state machine
   */
  def startup() {
    // initialize replica state 初始化zk上各个副本状态
    initializeReplicaState()
    // set started flag
    hasStarted.set(true)
    // move all Online replicas to Online 尝试将副本状态转为OnlineReplica
    handleStateChanges(controllerContext.allLiveReplicas(), OnlineReplica)

    info("Started replica state machine with initial state -> " + replicaState.toString())
  }

  // register ZK listeners of the replica state machine
  def registerListeners() {
    // register broker change listener
    registerBrokerChangeListener()
  }

  // de-register ZK listeners of the replica state machine
  def deregisterListeners() {
    // de-register broker change listener
    deregisterBrokerChangeListener()
  }

  /**
   * Invoked on controller shutdown.
   */
  def shutdown() {
    // reset started flag
    hasStarted.set(false)
    // reset replica state
    replicaState.clear()
    // de-register all ZK listeners
    deregisterListeners()

    info("Stopped replica state machine")
  }

  /**
    * 处理PartitionAndReplica状态
   * This API is invoked by the broker change controller callbacks and the startup API of the state machine
   * @param replicas     The list of replicas (brokers) that need to be transitioned to the target state 需要变更到目标状态的副本列表
   * @param targetState  The state that the replicas should be moved to 目标状态
   * The controller's allLeaders cache should have been updated before this
   */
  def handleStateChanges(replicas: Set[PartitionAndReplica], targetState: ReplicaState,
                         callbacks: Callbacks = (new CallbackBuilder).build) {
    if(replicas.size > 0) {
      info("Invoking state change to %s for replicas %s".format(targetState, replicas.mkString(",")))
      try {
        brokerRequestBatch.newBatch()
        // 遍历所有副本转换到目标状态
        replicas.foreach(r => handleStateChange(r, targetState, callbacks))
        // 向其他broker发送消息
        brokerRequestBatch.sendRequestsToBrokers(controller.epoch)
      }catch {
        case e: Throwable => error("Error while moving some replicas to %s state".format(targetState), e)
      }
    }
  }

  /**
   * This API exercises the replica's state machine. It ensures that every state transition happens from a legal
   * previous state to the target state. Valid state transitions are:
   * NonExistentReplica --> NewReplica
   * --send LeaderAndIsr request with current leader and isr to the new replica and UpdateMetadata request for the
   *   partition to every live broker
   *
   * NewReplica -> OnlineReplica
   * --add the new replica to the assigned replica list if needed
   *
   * OnlineReplica,OfflineReplica -> OnlineReplica
   * --send LeaderAndIsr request with current leader and isr to the new replica and UpdateMetadata request for the
   *   partition to every live broker
   *
   * NewReplica,OnlineReplica,OfflineReplica,ReplicaDeletionIneligible -> OfflineReplica
   * --send StopReplicaRequest to the replica (w/o deletion)
   * --remove this replica from the isr and send LeaderAndIsr request (with new isr) to the leader replica and
   *   UpdateMetadata request for the partition to every live broker.
   *
   * OfflineReplica -> ReplicaDeletionStarted
   * --send StopReplicaRequest to the replica (with deletion)
   *
   * ReplicaDeletionStarted -> ReplicaDeletionSuccessful
   * -- mark the state of the replica in the state machine
   *
   * ReplicaDeletionStarted -> ReplicaDeletionIneligible
   * -- mark the state of the replica in the state machine
   *
   * ReplicaDeletionSuccessful -> NonExistentReplica
   * -- remove the replica from the in memory partition replica assignment cache


   * @param partitionAndReplica The replica for which the state transition is invoked
   * @param targetState The end state that the replica should be moved to
   */
  def handleStateChange(partitionAndReplica: PartitionAndReplica, targetState: ReplicaState,
                        callbacks: Callbacks) {
    // 获取副本的主题、分区、副本id
    val topic = partitionAndReplica.topic
    val partition = partitionAndReplica.partition
    val replicaId = partitionAndReplica.replica
    val topicAndPartition = TopicAndPartition(topic, partition)
    // 校验 state machine状态
    if (!hasStarted.get)
      throw new StateChangeFailedException(("Controller %d epoch %d initiated state change of replica %d for partition %s " +
                                            "to %s failed because replica state machine has not started")
                                              .format(controllerId, controller.epoch, replicaId, topicAndPartition, targetState))
    // 获取当前副本的状态，如果不存在则为NonExistentReplica
    val currState = replicaState.getOrElseUpdate(partitionAndReplica, NonExistentReplica)
    try {
      // 获取topic-partition对应的ar副本列表
      val replicaAssignment = controllerContext.partitionReplicaAssignment(topicAndPartition)
      // 判断目标状态
      targetState match {
        case NewReplica =>
          // 校验副本的前置状态，只有处于 NonExistentReplica 状态的副本才能转移到 NewReplica 状态
          assertValidPreviousStates(partitionAndReplica, List(NonExistentReplica), targetState)
          // start replica as a follower to the current leader for its partition
          // 从ZK中获取该分区的 LeaderIsrAndControllerEpoch 信息
          val leaderIsrAndControllerEpochOpt = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, topic, partition)
          leaderIsrAndControllerEpochOpt match {
            case Some(leaderIsrAndControllerEpoch) =>
              // 获取到分区的 LeaderIsrAndControllerEpoch 信息，如果发现该分区的leader是当前副本，
              // 那么就抛出StateChangeFailedException异常，因为处在这个状态的副本是不能被选举为leader的
              if(leaderIsrAndControllerEpoch.leaderAndIsr.leader == replicaId)
                throw new StateChangeFailedException("Replica %d for partition %s cannot be moved to NewReplica"
                  .format(replicaId, topicAndPartition) + "state as it is being requested to become leader")
              // 获取到了分区的 LeaderIsrAndControllerEpoch 信息，并且分区的 leader 不是当前副本，那么向该分区的所有副本添加一个 LeaderAndIsr请求
              // （添加 LeaderAndIsr 请求时，同时也会向所有的 Broker 都添加一个 UpdateMetadata 请求）
              brokerRequestBatch.addLeaderAndIsrRequestForBrokers(List(replicaId),
                                                                  topic, partition, leaderIsrAndControllerEpoch,
                                                                  replicaAssignment)
            // 如果获取不到leaderIsrAndControllerEpochOpt信息，直接将该副本的状态设置为 NewReplica，
            // 然后结束流程（新建分区时，副本可能处于这个状态，该分区的所有副本是没有 LeaderAndIsr 信息的）
            case None => // new leader request will be sent to this replica when one gets elected
          }
          // 修改副本状态
          replicaState.put(partitionAndReplica, NewReplica)
          stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
                                    .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState,
                                            targetState))
        case ReplicaDeletionStarted =>
          assertValidPreviousStates(partitionAndReplica, List(OfflineReplica), targetState)
          // 修改副本状态
          replicaState.put(partitionAndReplica, ReplicaDeletionStarted)
          // send stop replica command
          // 准备要发送的StopReplicaRequest请求到这个分区所管理的副本
          brokerRequestBatch.addStopReplicaRequestForBrokers(List(replicaId), topic, partition, deletePartition = true,
            callbacks.stopReplicaResponseCallback)
          stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
            .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
        case ReplicaDeletionIneligible =>
          assertValidPreviousStates(partitionAndReplica, List(ReplicaDeletionStarted), targetState)
          // 修改副本状态
          replicaState.put(partitionAndReplica, ReplicaDeletionIneligible)
          stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
            .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
        case ReplicaDeletionSuccessful =>
          assertValidPreviousStates(partitionAndReplica, List(ReplicaDeletionStarted), targetState)
          // 修改副本状态
          replicaState.put(partitionAndReplica, ReplicaDeletionSuccessful)
          stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
            .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
        case NonExistentReplica =>
          assertValidPreviousStates(partitionAndReplica, List(ReplicaDeletionSuccessful), targetState)
          // remove this replica from the assigned replicas list for its partition
          // 获取当前topic-partiton的ar列表
          val currentAssignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
          // 剔除删除的副本并更新ar列表
          controllerContext.partitionReplicaAssignment.put(topicAndPartition, currentAssignedReplicas.filterNot(_ == replicaId))
          // 删除部分的状态
          replicaState.remove(partitionAndReplica)
          stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
            .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
        case OnlineReplica =>
          assertValidPreviousStates(partitionAndReplica,
            List(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible), targetState)
          replicaState(partitionAndReplica) match {
              // NewReplica转为OnlineReplica
            case NewReplica =>
              // add this replica to the assigned replicas list for its partition
              // 获取当前topic-partiton的ar列表
              val currentAssignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
              // 如果ar列表不包含那么将当前副本加进去
              if(!currentAssignedReplicas.contains(replicaId))
                controllerContext.partitionReplicaAssignment.put(topicAndPartition, currentAssignedReplicas :+ replicaId)
              stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
                                        .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState,
                                                targetState))
            case _ =>
              // check if the leader for this partition ever existed
              // 检查当前分区leader副本是否存在
              controllerContext.partitionLeadershipInfo.get(topicAndPartition) match {
                  // 存在
                case Some(leaderIsrAndControllerEpoch) =>
                  // 添加LeaderAndIsrRequest请求
                  brokerRequestBatch.addLeaderAndIsrRequestForBrokers(List(replicaId), topic, partition, leaderIsrAndControllerEpoch,
                    replicaAssignment)
                  // 修改副本状态
                  replicaState.put(partitionAndReplica, OnlineReplica)
                  stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
                    .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
                case None => // that means the partition was never in OnlinePartition state, this means the broker never
                  // started a log for that partition and does not have a high watermark value for this partition
              }
          }
          replicaState.put(partitionAndReplica, OnlineReplica)
        case OfflineReplica =>
          assertValidPreviousStates(partitionAndReplica,
            List(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible), targetState)
          // send stop replica command to the replica so that it stops fetching from the leader
          // 准备要发送的StopReplicaRequest请求到其他broker
          brokerRequestBatch.addStopReplicaRequestForBrokers(List(replicaId), topic, partition, deletePartition = false)
          // As an optimization, the controller removes dead replicas from the ISR
          val leaderAndIsrIsEmpty: Boolean =
            // 检查当前分区leader副本是否存在
            controllerContext.partitionLeadershipInfo.get(topicAndPartition) match {
              case Some(currLeaderIsrAndControllerEpoch) =>
                controller.removeReplicaFromIsr(topic, partition, replicaId) match {
                  case Some(updatedLeaderIsrAndControllerEpoch) =>
                    // send the shrunk ISR state change request to all the remaining alive replicas of the partition.
                    // 将收缩后的isr列表发送给该分区中所有剩余的活副本
                    val currentAssignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
                    if (!controller.deleteTopicManager.isPartitionToBeDeleted(topicAndPartition)) {
                      brokerRequestBatch.addLeaderAndIsrRequestForBrokers(currentAssignedReplicas.filterNot(_ == replicaId),
                        topic, partition, updatedLeaderIsrAndControllerEpoch, replicaAssignment)
                    }
                    // 修改副本状态
                    replicaState.put(partitionAndReplica, OfflineReplica)
                    stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
                      .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
                    false
                  case None =>
                    true
                }
              case None =>
                true
            }
          if (leaderAndIsrIsEmpty && !controller.deleteTopicManager.isPartitionToBeDeleted(topicAndPartition))
            throw new StateChangeFailedException(
              "Failed to change state of replica %d for partition %s since the leader and isr path in zookeeper is empty"
              .format(replicaId, topicAndPartition))
      }
    }
    catch {
      case t: Throwable =>
        stateChangeLogger.error("Controller %d epoch %d initiated state change of replica %d for partition [%s,%d] from %s to %s failed"
                                  .format(controllerId, controller.epoch, replicaId, topic, partition, currState, targetState), t)
    }
  }

  // 判断topic的所有分区的所有副本是否都已经删除
  // 也就是状态为ReplicaDeletionSuccessful
  def areAllReplicasForTopicDeleted(topic: String): Boolean = {
    // 获取指定topic上的PartitionAndReplica，返回内容是Set[PartitionAndReplica]集合
    val replicasForTopic = controller.controllerContext.replicasForTopic(topic)
    // 遍历topic-partition集合，获取副本状态
    // key是topic-partiton,value是ReplicaState
    val replicaStatesForTopic = replicasForTopic.map(r => (r, replicaState(r))).toMap
    debug("Are all replicas for topic %s deleted %s".format(topic, replicaStatesForTopic))
    // 判断是否topic-partition对应的状态为ReplicaDeletionSuccessful
    replicaStatesForTopic.forall(_._2 == ReplicaDeletionSuccessful)
  }

  // 判断是否至少有一个topic-partiton的副本的ReplicaState状态为ReplicaDeletionStarted
  def isAtLeastOneReplicaInDeletionStartedState(topic: String): Boolean = {
    // 获取指定topic上的PartitionAndReplica，返回内容是Set[PartitionAndReplica]集合
    val replicasForTopic = controller.controllerContext.replicasForTopic(topic)
    // 遍历topic-partition集合，获取副本状态
    // key是topic-partiton,value是ReplicaState
    val replicaStatesForTopic = replicasForTopic.map(r => (r, replicaState(r))).toMap
    // 判断是否至少有一个topic-partiton的副本的ReplicaState状态为ReplicaDeletionStarted
    replicaStatesForTopic.foldLeft(false)((deletionState, r) => deletionState || r._2 == ReplicaDeletionStarted)
  }

  // 过滤出指定topic的所有分区的所有topic-partition的状态为state的集合
  def replicasInState(topic: String, state: ReplicaState): Set[PartitionAndReplica] = {
    replicaState.filter(r => r._1.topic.equals(topic) && r._2 == state).keySet
  }

  // 参数topic的所有副本中是否有状态为state的
  def isAnyReplicaInState(topic: String, state: ReplicaState): Boolean = {
    replicaState.exists(r => r._1.topic.equals(topic) && r._2 == state)
  }

  def replicasInDeletionStates(topic: String): Set[PartitionAndReplica] = {
    val deletionStates = Set(ReplicaDeletionStarted, ReplicaDeletionSuccessful, ReplicaDeletionIneligible)
    replicaState.filter(r => r._1.topic.equals(topic) && deletionStates.contains(r._2)).keySet
  }

  /**
    * 验证状态
    * @param partitionAndReplica 分区副本
    * @param fromStates 分区副本原状态
    * @param targetState 分区副本目标状态
    */
  private def assertValidPreviousStates(partitionAndReplica: PartitionAndReplica, fromStates: Seq[ReplicaState],
                                        targetState: ReplicaState) {
    assert(fromStates.contains(replicaState(partitionAndReplica)),
      "Replica %s should be in the %s states before moving to %s state"
        .format(partitionAndReplica, fromStates.mkString(","), targetState) +
        ". Instead it is in %s state".format(replicaState(partitionAndReplica)))
  }

  private def registerBrokerChangeListener() = {
    zkUtils.zkClient.subscribeChildChanges(ZkUtils.BrokerIdsPath, brokerChangeListener)
  }

  private def deregisterBrokerChangeListener() = {
    zkUtils.zkClient.unsubscribeChildChanges(ZkUtils.BrokerIdsPath, brokerChangeListener)
  }

  /**
   * Invoked on startup of the replica's state machine to set the initial state for replicas of all existing partitions
   * in zookeeper
    * 初始化每个主题对应副本的状态
   */
  private def initializeReplicaState() {
    // 遍历zk上每一个主题分区的AR副本列表
    for((topicPartition, assignedReplicas) <- controllerContext.partitionReplicaAssignment) {
      // 获取主题
      val topic = topicPartition.topic
      // 获取分区
      val partition = topicPartition.partition
      // 遍历副本
      assignedReplicas.foreach { replicaId =>
        val partitionAndReplica = PartitionAndReplica(topic, partition, replicaId)
        // 判断live的副本集合中是否包含当前副本
        controllerContext.liveBrokerIds.contains(replicaId) match {
            // 如果包含则初始当前副本的状态为OnlineReplica
          case true => replicaState.put(partitionAndReplica, OnlineReplica)
          case false =>
            // mark replicas on dead brokers as failed for topic deletion, if they belong to a topic to be deleted.
            // This is required during controller failover since during controller failover a broker can go down,
            // so the replicas on that broker should be moved to ReplicaDeletionIneligible to be on the safer side.
            // 如果不包含，则初始化当前副本状态为ReplicaDeletionIneligible
            replicaState.put(partitionAndReplica, ReplicaDeletionIneligible)
        }
      }
    }
  }

  def partitionsAssignedToBroker(topics: Seq[String], brokerId: Int):Seq[TopicAndPartition] = {
    controllerContext.partitionReplicaAssignment.filter(_._2.contains(brokerId)).keySet.toSeq
  }

  /**
   * This is the zookeeper listener that triggers all the state transitions for a replica
    * 监听"/brokers/ids"
   */
  class BrokerChangeListener() extends IZkChildListener with Logging {
    this.logIdent = "[BrokerChangeListener on Controller " + controller.config.brokerId + "]: "
    def handleChildChange(parentPath : String, currentBrokerList : java.util.List[String]) {
      info("Broker change listener fired for path %s with children %s".format(parentPath, currentBrokerList.sorted.mkString(",")))
      inLock(controllerContext.controllerLock) {
        if (hasStarted.get) {
          ControllerStats.leaderElectionTimer.time {
            try {
              // 获取当前的broker列表
              val curBrokers = currentBrokerList.map(_.toInt).toSet.flatMap(zkUtils.getBrokerInfo)
              // 获取当前的brokerId列表
              val curBrokerIds = curBrokers.map(_.id)
              // 获取前者live的brokerId列表
              val liveOrShuttingDownBrokerIds = controllerContext.liveOrShuttingDownBrokerIds
              // 获取新的brokerId
              val newBrokerIds = curBrokerIds -- liveOrShuttingDownBrokerIds
              // 获取宕机的brokerId
              val deadBrokerIds = liveOrShuttingDownBrokerIds -- curBrokerIds
              // 获取新的broker
              val newBrokers = curBrokers.filter(broker => newBrokerIds(broker.id))
              // 更新controllerContext.liveBrokers
              controllerContext.liveBrokers = curBrokers
              // 根据brokerId排序
              val newBrokerIdsSorted = newBrokerIds.toSeq.sorted
              val deadBrokerIdsSorted = deadBrokerIds.toSeq.sorted
              val liveBrokerIdsSorted = curBrokerIds.toSeq.sorted
              info("Newly added brokers: %s, deleted brokers: %s, all live brokers: %s"
                .format(newBrokerIdsSorted.mkString(","), deadBrokerIdsSorted.mkString(","), liveBrokerIdsSorted.mkString(",")))
              // 为新的broker建立连接
              newBrokers.foreach(controllerContext.controllerChannelManager.addBroker)
              // 取消宕机的broker的连接
              deadBrokerIds.foreach(controllerContext.controllerChannelManager.removeBroker)
              if(newBrokerIds.size > 0)
                // 处理新增broker的上线
                controller.onBrokerStartup(newBrokerIdsSorted)
              if(deadBrokerIds.size > 0)
                // 处理宕机broker的下线
                controller.onBrokerFailure(deadBrokerIdsSorted)
            } catch {
              case e: Throwable => error("Error while handling broker changes", e)
            }
          }
        }
      }
    }
  }
}

sealed trait ReplicaState { def state: Byte }
case object NewReplica extends ReplicaState { val state: Byte = 1 }
case object OnlineReplica extends ReplicaState { val state: Byte = 2 }
case object OfflineReplica extends ReplicaState { val state: Byte = 3 }
case object ReplicaDeletionStarted extends ReplicaState { val state: Byte = 4}
case object ReplicaDeletionSuccessful extends ReplicaState { val state: Byte = 5}
case object ReplicaDeletionIneligible extends ReplicaState { val state: Byte = 6}
case object NonExistentReplica extends ReplicaState { val state: Byte = 7 }
