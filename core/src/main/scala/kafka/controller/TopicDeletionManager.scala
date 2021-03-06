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


import kafka.server.ConfigType
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{StopReplicaResponse, AbstractRequestResponse}

import collection.mutable
import collection.JavaConverters._
import kafka.utils.{ShutdownableThread, Logging}
import kafka.utils.CoreUtils._
import kafka.utils.ZkUtils._
import collection.Set
import kafka.common.TopicAndPartition
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.atomic.AtomicBoolean

/**
 * This manages the state machine for topic deletion.
  * topic删除的状态机
 * 1. TopicCommand issues topic deletion by creating a new admin path /admin/delete_topics/<topic>
  *   删除topic可以在'/admin/delete_topics/{topic}'建立当前的节点
 * 2. The controller listens for child changes on /admin/delete_topic and starts topic deletion for the respective topics
  *  DeleteTopicsListener会监听'/admin/delete_topic'节点
 * 3. The controller has a background thread that handles topic deletion. The purpose of having this background thread
 *    is to accommodate the TTL feature, when we have it. This thread is signaled whenever deletion for a topic needs to
 *    be started or resumed. Currently, a topic's deletion can be started only by the onPartitionDeletion callback on the
 *    controller. In the future, it can be triggered based on the configured TTL for the topic. A topic will be ineligible
 *    for deletion in the following scenarios -
  *    在以下情景中topic将不合适被删除
 *    3.1 broker hosting one of the replicas for that topic goes down
  *    当前主题对应的副本宕机
 *    3.2 partition reassignment for partitions of that topic is in progress
  *    当前主题对应的分区正在重分配
 *    3.3 preferred replica election for partitions of that topic is in progress
  *    当前主题对应的分区正在leader选举
 *    (though this is not strictly required since it holds the controller lock for the entire duration from start to end)
 * 4. Topic deletion is resumed when -
  * 以下场景恢复主题的删除
 *    4.1 broker hosting one of the replicas for that topic is started  当前topic的任意一个副本启动
 *    4.2 preferred replica election for partitions of that topic completes 完成了topic的分区leader选举
 *    4.3 partition reassignment for partitions of that topic completes 完成了topic的分区重分配
 * 5. Every replica for a topic being deleted is in either of the 3 states -
  * 被删除topic的副本都会处于以下三个状态中的任一一种
 *    5.1 TopicDeletionStarted (Replica enters TopicDeletionStarted phase when the onPartitionDeletion callback is invoked.
 *        This happens when the child change watch for /admin/delete_topics fires on the controller. As part of this state
 *        change, the controller sends StopReplicaRequests to all replicas. It registers a callback for the
 *        StopReplicaResponse when deletePartition=true thereby invoking a callback when a response for delete replica
 *        is received from every replica)
 *    5.2 TopicDeletionSuccessful (deleteTopicStopReplicaCallback() moves replicas from
 *        TopicDeletionStarted->TopicDeletionSuccessful depending on the error codes in StopReplicaResponse)
 *    5.3 TopicDeletionFailed. (deleteTopicStopReplicaCallback() moves replicas from
 *        TopicDeletionStarted->TopicDeletionFailed depending on the error codes in StopReplicaResponse.
 *        In general, if a broker dies and if it hosted replicas for topics being deleted, the controller marks the
 *        respective replicas in TopicDeletionFailed state in the onBrokerFailure callback. The reason is that if a
 *        broker fails before the request is sent and after the replica is in TopicDeletionStarted state,
 *        it is possible that the replica will mistakenly remain in TopicDeletionStarted state and topic deletion
 *        will not be retried when the broker comes back up.)
 * 6. The delete topic thread marks a topic successfully deleted only if all replicas are in TopicDeletionSuccessful
 *    state and it starts the topic deletion teardown mode where it deletes all topic state from the controllerContext
 *    as well as from zookeeper. This is the only time the /brokers/topics/<topic> path gets deleted. On the other hand,
 *    if no replica is in TopicDeletionStarted state and at least one replica is in TopicDeletionFailed state, then
 *    it marks the topic for deletion retry.
  *    如果没有副本处于TopicDeletionStarted状态但是至少有一个副本处于TopicDeletionFailed状态，那么会标记当前topic需要重新删除
 * @param controller
 * @param initialTopicsToBeDeleted The topics that are queued up for deletion in zookeeper at the time of controller failover
 * @param initialTopicsIneligibleForDeletion The topics ineligible for deletion due to any of the conditions mentioned in #3 above
 */
class TopicDeletionManager(controller: KafkaController,
                           initialTopicsToBeDeleted: Set[String] = Set.empty,// 需要被删除的topic
                           initialTopicsIneligibleForDeletion: Set[String] = Set.empty) extends Logging {// 不适合被删除的topic
  this.logIdent = "[Topic Deletion Manager " + controller.config.brokerId + "], "
  // 用于维护KafkaController中上下文信息
  val controllerContext = controller.controllerContext
  // partition状态机
  val partitionStateMachine = controller.partitionStateMachine
  // replica状态机
  val replicaStateMachine = controller.replicaStateMachine
  // 待删除的topic
  val topicsToBeDeleted: mutable.Set[String] = mutable.Set.empty[String] ++ initialTopicsToBeDeleted
  // 待删除的topic的partition：topic-partition
  val partitionsToBeDeleted: mutable.Set[TopicAndPartition] = topicsToBeDeleted.flatMap(controllerContext.partitionsForTopic)
  // 锁
  val deleteLock = new ReentrantLock()
  // 暂停被删除的topic
  val topicsIneligibleForDeletion: mutable.Set[String] = mutable.Set.empty[String] ++
    (initialTopicsIneligibleForDeletion & initialTopicsToBeDeleted)
  // 删除topic线程条件通知
  val deleteTopicsCond = deleteLock.newCondition()
  // 标记topic删除是否发生，如果存在需要删除的topic时会被置为true
  val deleteTopicStateChanged: AtomicBoolean = new AtomicBoolean(false)
  // 删除topic的后台线程
  var deleteTopicsThread: DeleteTopicsThread = null
  //  "delete.topic.enable"是否允许删除topic
  val isDeleteTopicEnabled = controller.config.deleteTopicEnable

  /**
   * Invoked at the end of new controller initiation
    * 启动
   */
  def start() {
    // 如果"topic.delete.enable"=true，则创建并启动自动删除topic线程
    if (isDeleteTopicEnabled) {
      deleteTopicsThread = new DeleteTopicsThread()
      if (topicsToBeDeleted.size > 0)
        // 设置启动标识符为true
        deleteTopicStateChanged.set(true)
      // 启动线程
      deleteTopicsThread.start()
    }
  }

  /**
   * Invoked when the current controller resigns. At this time, all state for topic deletion should be cleared.
    * 关闭
   */
  def shutdown() {
    // Only allow one shutdown to go through
    // 如果"topic.delete.enable"=true && 关闭deleteTopicsThread成功
    if (isDeleteTopicEnabled && deleteTopicsThread.initiateShutdown()) {
      // Resume the topic deletion so it doesn't block on the condition
      // 此时删除线程有可能处于等待状态，即awaitTopicDeletionNotification方法处于阻塞等待状态，则唤醒该删除线程
      resumeTopicDeletionThread()
      // Await delete topic thread to exit
      // 等待删除线程doWork执行结束
      deleteTopicsThread.awaitShutdown()
      // 清除相关资源
      topicsToBeDeleted.clear()
      partitionsToBeDeleted.clear()
      topicsIneligibleForDeletion.clear()
    }
  }

  /**
   * Invoked by the child change listener on /admin/delete_topics to queue up the topics for deletion. The topic gets added
   * to the topicsToBeDeleted list and only gets removed from the list when the topic deletion has completed successfully
   * i.e. all replicas of all partitions of that topic are deleted successfully.
   * @param topics Topics that should be deleted
    * 将待删除的分区入队
   */
  def enqueueTopicsForDeletion(topics: Set[String]) {
    if(isDeleteTopicEnabled) {
      topicsToBeDeleted ++= topics
      partitionsToBeDeleted ++= topics.flatMap(controllerContext.partitionsForTopic)
      // 唤醒删除线程
      resumeTopicDeletionThread()
    }
  }

  /**
   * Invoked when any event that can possibly resume topic deletion occurs. These events include -
   * 1. New broker starts up. Any replicas belonging to topics queued up for deletion can be deleted since the broker is up
   * 2. Partition reassignment completes. Any partitions belonging to topics queued up for deletion finished reassignment
   * 3. Preferred replica election completes. Any partitions belonging to topics queued up for deletion finished
   *    preferred replica election
   * @param topics Topics for which deletion can be resumed
    *  暂停被删除的副本
   */
  def resumeDeletionForTopics(topics: Set[String] = Set.empty) {
    if(isDeleteTopicEnabled) {
      val topicsToResumeDeletion = topics & topicsToBeDeleted
      if(topicsToResumeDeletion.size > 0) {
        topicsIneligibleForDeletion --= topicsToResumeDeletion
        resumeTopicDeletionThread()
      }
    }
  }

  /**
   * Invoked when a broker that hosts replicas for topics to be deleted goes down. Also invoked when the callback for
   * StopReplicaResponse receives an error code for the replicas of a topic to be deleted. As part of this, the replicas
   * are moved from ReplicaDeletionStarted to ReplicaDeletionIneligible state. Also, the topic is added to the list of topics
   * ineligible for deletion until further notice. The delete topic thread is notified so it can retry topic deletion
   * if it has received a response for all replicas of a topic to be deleted
   * @param replicas Replicas for which deletion has failed
   */
  // 处理删除失败的副本
  // 1.标记删除失败的副本状态为ReplicaDeletionIneligible
  // 2.暂停topic的删除
  def failReplicaDeletion(replicas: Set[PartitionAndReplica]) {
    if(isDeleteTopicEnabled) {
      // 过滤出副本所处的topic在删除队列中的副本
      val replicasThatFailedToDelete = replicas.filter(r => isTopicQueuedUpForDeletion(r.topic))
      if(replicasThatFailedToDelete.size > 0) {
        val topics = replicasThatFailedToDelete.map(_.topic)
        debug("Deletion failed for replicas %s. Halting deletion for topics %s"
          .format(replicasThatFailedToDelete.mkString(","), topics))
        // 设置副本状态为ReplicaDeletionIneligible
        controller.replicaStateMachine.handleStateChanges(replicasThatFailedToDelete, ReplicaDeletionIneligible)
        // 标记topic暂停删除
        markTopicIneligibleForDeletion(topics)
        // 通知delete-topic-thread处理主题删除
        resumeTopicDeletionThread()
      }
    }
  }

  /**
   * Halt delete topic if -
    * 暂停删除的主题，包括以下情况
   * 1. replicas being down 副本下线
   * 2. partition reassignment in progress for some partitions of the topic 真正进行分区重分配
   * 3. preferred replica election in progress for some partitions of the topic 真正进行分区leader选举
   * @param topics Topics that should be marked ineligible for deletion. No op if the topic is was not previously queued up for deletion
   */
  def markTopicIneligibleForDeletion(topics: Set[String]) {
    if(isDeleteTopicEnabled) {
      val newTopicsToHaltDeletion = topicsToBeDeleted & topics
      topicsIneligibleForDeletion ++= newTopicsToHaltDeletion
      if(newTopicsToHaltDeletion.size > 0)
        info("Halted deletion of topics %s".format(newTopicsToHaltDeletion.mkString(",")))
    }
  }
  // 判断当前topic是否是暂停删除的topic
  def isTopicIneligibleForDeletion(topic: String): Boolean = {
    if(isDeleteTopicEnabled) {
      topicsIneligibleForDeletion.contains(topic)
    } else
      true
  }
  // 判断当前topic是否正在删除
  def isTopicDeletionInProgress(topic: String): Boolean = {
    if(isDeleteTopicEnabled) {
      // 判断是否至少有一个topic-partiton的副本的ReplicaState状态为ReplicaDeletionStarted
      controller.replicaStateMachine.isAtLeastOneReplicaInDeletionStartedState(topic)
    } else
      false
  }

  // 判断当前分区是否是需要删除的分区
  def isPartitionToBeDeleted(topicAndPartition: TopicAndPartition) = {
    if(isDeleteTopicEnabled) {
      partitionsToBeDeleted.contains(topicAndPartition)
    } else
      false
  }

  // 判断当前topic是否是需要删除的topic
  def isTopicQueuedUpForDeletion(topic: String): Boolean = {
    if(isDeleteTopicEnabled) {
      topicsToBeDeleted.contains(topic)
    } else
      false
  }

  /**
   * Invoked by the delete-topic-thread to wait until events that either trigger, restart or halt topic deletion occur.
   * controllerLock should be acquired before invoking this API
   */
  // 由delete-topic-thread调用，以等待事件触发
  private def awaitTopicDeletionNotification() {
    inLock(deleteLock) {
      // 1.如果删除topic的线程已经启动
      // 2.没有删除topic的操作发生
      // 那么阻塞delete-topic-thread线程的执行
      while(deleteTopicsThread.isRunning.get() && !deleteTopicStateChanged.compareAndSet(true, false)) {
        debug("Waiting for signal to start or continue topic deletion")
        deleteTopicsCond.await()
      }
    }
  }

  /**
   * Signals the delete-topic-thread to process topic deletion
    * 通知delete-topic-thread处理主题删除
   */
  private def resumeTopicDeletionThread() {
    deleteTopicStateChanged.set(true)
    inLock(deleteLock) {
      deleteTopicsCond.signal()
    }
  }

  /**
   * Invoked by the StopReplicaResponse callback when it receives no error code for a replica of a topic to be deleted.
   * As part of this, the replicas are moved from ReplicaDeletionStarted to ReplicaDeletionSuccessful state. The delete
   * topic thread is notified so it can tear down the topic if all replicas of a topic have been successfully deleted
   * @param replicas Replicas that were successfully deleted by the broker
   */
  // 获取状态为ReplicaDeletionSuccessful的副本
  private def completeReplicaDeletion(replicas: Set[PartitionAndReplica]) {
    val successfullyDeletedReplicas = replicas.filter(r => isTopicQueuedUpForDeletion(r.topic))
    debug("Deletion successfully completed for replicas %s".format(successfullyDeletedReplicas.mkString(",")))
    controller.replicaStateMachine.handleStateChanges(successfullyDeletedReplicas, ReplicaDeletionSuccessful)
    resumeTopicDeletionThread()
  }

  /**
   * Topic deletion can be retried if -
   * 1. Topic deletion is not already complete
   * 2. Topic deletion is currently not in progress for that topic
   * 3. Topic is currently marked ineligible for deletion
   * @param topic Topic
   * @return Whether or not deletion can be retried for the topic
   */
  // 如果是需要删除的topic && 当前topic的所有副本状态均不是ReplicaDeletionStarted && 当前topic没有被暂停删除
  private def isTopicEligibleForDeletion(topic: String): Boolean = {
    topicsToBeDeleted.contains(topic) && (!isTopicDeletionInProgress(topic) && !isTopicIneligibleForDeletion(topic))
  }

  /**
   * If the topic is queued for deletion but deletion is not currently under progress, then deletion is retried for that topic
   * To ensure a successful retry, reset states for respective replicas from ReplicaDeletionIneligible to OfflineReplica state
   *@param topic Topic for which deletion should be retried
   */
  // 重试删除topic
  private def markTopicForDeletionRetry(topic: String) {
    // reset replica states from ReplicaDeletionIneligible to OfflineReplica
    // 获取topic所有副本中状态为ReplicaDeletionIneligible的
    val failedReplicas = controller.replicaStateMachine.replicasInState(topic, ReplicaDeletionIneligible)
    info("Retrying delete topic for topic %s since replicas %s were not successfully deleted"
      .format(topic, failedReplicas.mkString(",")))
    // 将副本状态由ReplicaDeletionIneligible改为OfflineReplica
    controller.replicaStateMachine.handleStateChanges(failedReplicas, OfflineReplica)
  }

  // 删除topic
  private def completeDeleteTopic(topic: String) {
    // deregister partition change listener on the deleted topic. This is to prevent the partition change listener
    // firing before the new topic listener when a deleted topic gets auto created
    // 注销分区变更监听器，防止删除过程中因分区数据变更导致监听器被触发，引起状态不一致
    partitionStateMachine.deregisterPartitionChangeListener(topic)
    // 过滤出副本状态为ReplicaDeletionSuccessful的Set[PartitionAndReplica]集合,即所有已经被成功删除的副本对象
    val replicasForDeletedTopic = controller.replicaStateMachine.replicasInState(topic, ReplicaDeletionSuccessful)
    // controller will remove this replica from the state machine as well as its partition assignment cache
    // 转换Set[PartitionAndReplica]集合中所有副本的状态为NonExistentReplica,等同于在状态机中删除这些副本
    replicaStateMachine.handleStateChanges(replicasForDeletedTopic, NonExistentReplica)
    // 获取需要删除topic上的TopicAndPartition
    val partitionsForDeletedTopic = controllerContext.partitionsForTopic(topic)
    // move respective partition to OfflinePartition and NonExistentPartition state
    // 处理这些分区的状态为OfflinePartition 到 NonExistentPartition
    partitionStateMachine.handleStateChanges(partitionsForDeletedTopic, OfflinePartition)
    partitionStateMachine.handleStateChanges(partitionsForDeletedTopic, NonExistentPartition)
    // 删除记录的topic
    topicsToBeDeleted -= topic
    // 删除partitionsToBeDeleted中的记录
    partitionsToBeDeleted.retain(_.topic != topic)
    val zkUtils = controllerContext.zkUtils
    // 删除zookeeper上节点/brokers/topics/{topic_name}
    zkUtils.zkClient.deleteRecursive(getTopicPath(topic))
    // 删除zookeeper上节点/config/{entityType}/{entity}
    zkUtils.zkClient.deleteRecursive(getEntityConfigPath(ConfigType.Topic, topic))
    // 删除zookeeper上节点/admin/delete_topics/{topic_name}
    zkUtils.zkClient.delete(getDeleteTopicPath(topic))
    // 最后移除controllerContext中的topic相关信息
    controllerContext.removeTopic(topic)
  }

  /**
   * This callback is invoked by the DeleteTopics thread with the list of topics to be deleted
   * It invokes the delete partition callback for all partitions of a topic.
   * The updateMetadataRequest is also going to set the leader for the topics being deleted to
   * {@link LeaderAndIsr#LeaderDuringDelete}. This lets each broker know that this topic is being deleted and can be
   * removed from their caches.
   */
  // 开始删除topic
  private def onTopicDeletion(topics: Set[String]) {
    info("Topic deletion callback for %s".format(topics.mkString(",")))
    // send update metadata so that brokers stop serving data for topics to be deleted
    // 获取所有待删除topic的TopicAndPartition，返回类型Set[TopicAndPartition]
    // 也就是分区信息
    val partitions = topics.flatMap(controllerContext.partitionsForTopic)
    // 发送UpdateMetadataRequest请求给所有的broker节点，告诉它们删除自己本地记录的关于这些分区的信息
    // 参考：kafka.server.MetadataCache.updateCache
    controller.sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, partitions)
    // 将本地记录的所有分区的副本集合根据topic进行分组
    // 返回类型为：Map[topic,Map[TopicAndPartition, Seq[Int]]]
    val partitionReplicaAssignmentByTopic = controllerContext.partitionReplicaAssignment.groupBy(p => p._1.topic)
    // 遍历待删除的topic
    topics.foreach { topic =>
      // 获取每一个topic的Set[TopicAndPartition]也就是副本集合传递给onPartitionDeletion方法
      onPartitionDeletion(partitionReplicaAssignmentByTopic(topic).map(_._1).toSet)
    }
  }

  /**
   * Invoked by the onPartitionDeletion callback. It is the 2nd step of topic deletion, the first being sending
   * UpdateMetadata requests to all brokers to start rejecting requests for deleted topics. As part of starting deletion,
   * the topics are added to the in progress list. As long as a topic is in the in progress list, deletion for that topic
   * is never retried. A topic is removed from the in progress list when
   * 1. Either the topic is successfully deleted OR
   * 2. No replica for the topic is in ReplicaDeletionStarted state and at least one replica is in ReplicaDeletionIneligible state
   * If the topic is queued for deletion but deletion is not currently under progress, then deletion is retried for that topic
   * As part of starting deletion, all replicas are moved to the ReplicaDeletionStarted state where the controller sends
   * the replicas a StopReplicaRequest (delete=true)
   * This callback does the following things -
   * 1. Move all dead replicas directly to ReplicaDeletionIneligible state. Also mark the respective topics ineligible
   *    for deletion if some replicas are dead since it won't complete successfully anyway
   * 2. Move all alive replicas to ReplicaDeletionStarted state so they can be deleted successfully
   *@param replicasForTopicsToBeDeleted
   */
  // 开始删除副本
  private def startReplicaDeletion(replicasForTopicsToBeDeleted: Set[PartitionAndReplica]) {
    // 遍历待删除的副本
    replicasForTopicsToBeDeleted.groupBy(_.topic).foreach { case(topic, replicas) =>
      // 过滤出分区所有副本
      var aliveReplicasForTopic = controllerContext.allLiveReplicas().filter(p => p.topic.equals(topic))
      // 过滤出分区所有副本中处于Dead状态的
      val deadReplicasForTopic = replicasForTopicsToBeDeleted -- aliveReplicasForTopic
      // 过滤出分区所有副本中处于ReplicaDeletionSuccessful状态的
      val successfullyDeletedReplicas = controller.replicaStateMachine.replicasInState(topic, ReplicaDeletionSuccessful)
      // 过滤出分区所有副本中未完成删除的
      val replicasForDeletionRetry = aliveReplicasForTopic -- successfullyDeletedReplicas
      // move dead replicas directly to failed state
      // 将不可用副本的状态标记为ReplicaDeletionIneligible
      replicaStateMachine.handleStateChanges(deadReplicasForTopic, ReplicaDeletionIneligible)
      // send stop replica to all followers that are not in the OfflineReplica state so they stop sending fetch requests to the leader
      // 将未完成删除的副本的状态标记为OfflineReplica
      replicaStateMachine.handleStateChanges(replicasForDeletionRetry, OfflineReplica)
      debug("Deletion started for replicas %s".format(replicasForDeletionRetry.mkString(",")))
      // 将待删除的副本状态标记为ReplicaDeletionStarted，标记当前副本准备好开始删除
      // 并发送StopReplicaRequest请求到这个分区所管理的副本上，然后等待响应后的回调
      controller.replicaStateMachine.handleStateChanges(replicasForDeletionRetry, ReplicaDeletionStarted,
        new Callbacks.CallbackBuilder().stopReplicaCallback(deleteTopicStopReplicaCallback).build)
      // 如果当前topic存在不可用的副本，标记topic暂停删除
      if(deadReplicasForTopic.size > 0) {
        debug("Dead Replicas (%s) found for topic %s".format(deadReplicasForTopic.mkString(","), topic))
        markTopicIneligibleForDeletion(Set(topic))
      }
    }
  }

  /**
   * This callback is invoked by the delete topic callback with the list of partitions for topics to be deleted
   * It does the following -
   * 1. Send UpdateMetadataRequest to all live brokers (that are not shutting down) for partitions that are being
   *    deleted. The brokers start rejecting all client requests with UnknownTopicOrPartitionException
   * 2. Move all replicas for the partitions to OfflineReplica state. This will send StopReplicaRequest to the replicas
   *    and LeaderAndIsrRequest to the leader with the shrunk ISR. When the leader replica itself is moved to OfflineReplica state,
   *    it will skip sending the LeaderAndIsrRequest since the leader will be updated to -1
   * 3. Move all replicas to ReplicaDeletionStarted state. This will send StopReplicaRequest with deletePartition=true. And
   *    will delete all persistent data from all replicas of the respective partitions
   */
  // 当删除分区时触发，删除分区对应的副本
  private def onPartitionDeletion(partitionsToBeDeleted: Set[TopicAndPartition]) {
    info("Partition deletion callback for %s".format(partitionsToBeDeleted.mkString(",")))
    // 将参数将Set[TopicAndPartition]转换为Set[PartitionAndReplica]，
    // 获取每个分区的所有副本，然后将这些副本封装到集合中返回一个Set[PartitionAndReplica]
    val replicasPerPartition = controllerContext.replicasForPartition(partitionsToBeDeleted)
    // 开始删除副本
    startReplicaDeletion(replicasPerPartition)
  }

  // 删除副本后的回调
  private def deleteTopicStopReplicaCallback(stopReplicaResponseObj: AbstractRequestResponse, replicaId: Int) {
    // 获取StopReplicaRequest请求的响应
    val stopReplicaResponse = stopReplicaResponseObj.asInstanceOf[StopReplicaResponse]
    debug("Delete topic callback invoked for %s".format(stopReplicaResponse))
    val responseMap = stopReplicaResponse.responses.asScala
    // 获取删除失败的分区集合
    val partitionsInError =
      if (stopReplicaResponse.errorCode != Errors.NONE.code) responseMap.keySet
      else responseMap.filter { case (_, error) => error != Errors.NONE.code }.map(_._1).toSet
    // 将删除失败的分区集合转换为副本集合
    val replicasInError = partitionsInError.map(p => PartitionAndReplica(p.topic, p.partition, replicaId))
    inLock(controllerContext.controllerLock) {
      // move all the failed replicas to ReplicaDeletionIneligible
      // 处理删除失败的副本
      failReplicaDeletion(replicasInError)
      // 成立说明有些副本删除成功
      if (replicasInError.size != responseMap.size) {
        // some replicas could have been successfully deleted
        // 有些副本删除成功
        // 计算删除成功的副本id
        val deletedReplicas = responseMap.keySet -- partitionsInError
        // 将删除成功的副本状态标记为ReplicaDeletionSuccessful
        completeReplicaDeletion(deletedReplicas.map(p => PartitionAndReplica(p.topic, p.partition, replicaId)))
      }
    }
  }

  /**
    * 删除topic线程
    */
  class DeleteTopicsThread() extends ShutdownableThread(name = "delete-topics-thread-" + controller.config.brokerId, isInterruptible = false) {
    val zkUtils = controllerContext.zkUtils
    // 删除操作
    override def doWork() {
      // 等待删除Topic的事件通知
      awaitTopicDeletionNotification()
      // 为启动返回
      if (!isRunning.get)
        return
      // 加锁
      inLock(controllerContext.controllerLock) {
        // 获取待删除的topic
        val topicsQueuedForDeletion = Set.empty[String] ++ topicsToBeDeleted
        // 如果有待删除的topic，打印日志
        if(!topicsQueuedForDeletion.isEmpty)
          info("Handling deletion for topics " + topicsQueuedForDeletion.mkString(","))
        // 遍历所有待删除的topic
        topicsQueuedForDeletion.foreach { topic =>
        // if all replicas are marked as deleted successfully, then topic deletion is done
          // 判断topic的所有分区的所有副本是否都已经删除，副本状态为ReplicaDeletionSuccessful
          if(controller.replicaStateMachine.areAllReplicasForTopicDeleted(topic)) {
            // clear up all state for this topic from controller cache and zookeeper
            // 删除topic
            completeDeleteTopic(topic)
            info("Deletion of topic %s successfully completed".format(topic))
          } else {
            // 判断是否至少有一个副本的状态为ReplicaDeletionStarted
            if(controller.replicaStateMachine.isAtLeastOneReplicaInDeletionStartedState(topic)) {
              // ignore since topic deletion is in progress
              // 过滤出topic所有副本中，状态为ReplicaDeletionStarted的
              // 返回类型为Set[PartitionAndReplica]
              val replicasInDeletionStartedState = controller.replicaStateMachine.replicasInState(topic, ReplicaDeletionStarted)
              // 获取状态为ReplicaDeletionStarted的副本id集合
              val replicaIds = replicasInDeletionStartedState.map(_.replica)
              // 获取状态为ReplicaDeletionStarted的副本所在的分区集合
              val partitions = replicasInDeletionStartedState.map(r => TopicAndPartition(r.topic, r.partition))
              // 打印日志即可
              info("Deletion for replicas %s for partition %s of topic %s in progress".format(replicaIds.mkString(","),
                partitions.mkString(","), topic))
            } else {
              // if you come here, then no replica is in TopicDeletionStarted and all replicas are not in
              // TopicDeletionSuccessful. That means, that either given topic haven't initiated deletion
              // or there is at least one failed replica (which means topic deletion should be retried).
              // 判断topic的副本是否处于ReplicaDeletionIneligible状态的，如果有就尝试执行重试
              if(controller.replicaStateMachine.isAnyReplicaInState(topic, ReplicaDeletionIneligible)) {
                // mark topic for deletion retry
                // 标记topic重新删除
                markTopicForDeletionRetry(topic)
              }
            }
          }
          // Try delete topic if it is eligible for deletion.
          // 检查当前topic是否可以删除
          // 如果是需要删除的topic && 当前topic的所有副本状态均不是ReplicaDeletionStarted && 当前topic没有被暂停删除
          if(isTopicEligibleForDeletion(topic)) {
            info("Deletion of topic %s (re)started".format(topic))
            // topic deletion will be kicked off
            // 开始删除topic
            onTopicDeletion(Set(topic))
          } else if(isTopicIneligibleForDeletion(topic)) {
            info("Not retrying deletion of topic %s at this time since it is marked ineligible for deletion".format(topic))
          }
        }
      }
    }
  }
}
