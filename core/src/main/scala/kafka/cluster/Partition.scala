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
package kafka.cluster

import kafka.common._
import kafka.utils._
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.admin.AdminUtils
import kafka.api.LeaderAndIsr
import kafka.log.{LogAppendInfo, LogConfig}
import kafka.server._
import kafka.metrics.KafkaMetricsGroup
import kafka.controller.KafkaController
import kafka.message.ByteBufferMessageSet
import java.io.IOException
import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.kafka.common.errors.{NotEnoughReplicasException, NotLeaderForPartitionException}
import org.apache.kafka.common.protocol.Errors

import scala.collection.JavaConverters._
import com.yammer.metrics.core.Gauge
import org.apache.kafka.common.requests.PartitionState

import scala.collection.mutable

/**
 * Data structure that represents a topic partition. The leader maintains the AR, ISR, CUR, RAR
  * 表示topic partition的结构，leader节点用来维护AR, ISR, CUR, RAR
  * 定义topic后会设置多个partition，每个partition有通过多个broker构建leader和follower的集合体
 */
class Partition(val topic: String,// topic
                val partitionId: Int,// partition ID，也就是编号
                time: Time,
                replicaManager: ReplicaManager) extends Logging with KafkaMetricsGroup {// 当前broker上的ReplicaManager
  // brokerID
  private val localBrokerId = replicaManager.config.brokerId
  // 日志管理器
  private val logManager = replicaManager.logManager
  // zk工具
  private val zkUtils = replicaManager.zkUtils
  // 记录topic-partition里的副本集 key是副本ID，value是副本实例
  private val assignedReplicaMap = new Pool[Int, Replica]
  // The read lock is only required when multiple reads are executed and needs to be in a consistent manner
  // 在处理当前分区的ISR列表时，需要加锁
  private val leaderIsrUpdateLock = new ReentrantReadWriteLock()
  // zk版本，每次ISR列表发生变更时会更新，默认0，在updateIsr()方法中被更新
  private var zkVersion: Int = LeaderAndIsr.initialZKVersion
  // topic-partition副本集中的leader副本的epoch，当leader发生变更后会更新，默认-1
  @volatile private var leaderEpoch: Int = LeaderAndIsr.initialLeaderEpoch - 1
  // 记录topic-partition的leader副本ID
  @volatile var leaderReplicaIdOpt: Option[Int] = None
  // ISR列表(如果是follower该集合为空)，在updateIsr()方法中被更新
  @volatile var inSyncReplicas: Set[Replica] = Set.empty[Replica]

  /* Epoch of the controller that last changed the leader. This needs to be initialized correctly upon broker startup.
   * One way of doing that is through the controller's start replica state change command. When a new broker starts up
   * the controller sends it a start replica command containing the leader for each partition that the broker hosts.
   * In addition to the leader, the controller can also send the epoch of the controller that elected the leader for
   * each partition. */
  // 记录controller的epoch
  private var controllerEpoch: Int = KafkaController.InitialControllerEpoch - 1
  this.logIdent = "Partition [%s,%d] on broker %d: ".format(topic, partitionId, localBrokerId)

  private def isReplicaLocal(replicaId: Int) : Boolean = (replicaId == localBrokerId)
  val tags = Map("topic" -> topic, "partition" -> partitionId.toString)

  newGauge("UnderReplicated",
    new Gauge[Int] {
      def value = {
        if (isUnderReplicated) 1 else 0
      }
    },
    tags
  )

  // 当前topic-partition是否有副本没跟上
  def isUnderReplicated(): Boolean = {
    leaderReplicaIfLocal() match {
      case Some(_) =>
        inSyncReplicas.size < assignedReplicas.size
      case None =>
        false
    }
  }

  // 获取或者创建Replica实例
  def getOrCreateReplica(replicaId: Int = localBrokerId): Replica = {
    // 从assignedReplicaMap列表获取对应的Replica实例信息
    val replicaOpt = getReplica(replicaId)
    replicaOpt match {
        // 有就返回
      case Some(replica) => replica
      case None =>
        // 如果参数replicaId等于当前brokerId并
        if (isReplicaLocal(replicaId)) {
          // 获取该topic-partition对应的Log目录
          val config = LogConfig.fromProps(logManager.defaultConfig.originals, AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic, topic))
          val log = logManager.createLog(TopicAndPartition(topic, partitionId), config)
          // 获取replication-offset-checkpoint文件的OffsetCheckpoint实例信息
          val checkpoint: OffsetCheckpoint = replicaManager.highWatermarkCheckpoints(log.dir.getParentFile.getAbsolutePath)
          val offsetMap = checkpoint.read
          if (!offsetMap.contains(TopicAndPartition(topic, partitionId)))
            info("No checkpointed highwatermark is found for partition [%s,%d]".format(topic, partitionId))
          // 从replication-offset-checkpoint文件中获取当前broker上记录的该topic-partition的hw
          val offset = offsetMap.getOrElse(TopicAndPartition(topic, partitionId), 0L).min(log.logEndOffset)
          // 构建Replica副本实例
          val localReplica = new Replica(replicaId, this, time, offset, Some(log))
          addReplicaIfNotExists(localReplica)
          // 如果参数replicaId记录的不是当前broker，也就是说replicaId是远程的一个副本
        } else {
          // 创建一个Replica并添加到assignedReplicaMap中，这里HW置为0，LEO置为-1
          val remoteReplica = new Replica(replicaId, this, time)
          addReplicaIfNotExists(remoteReplica)
        }
        getReplica(replicaId).get
    }
  }

  // 根据副本ID获取对应的副本信息
  def getReplica(replicaId: Int = localBrokerId): Option[Replica] = {
    val replica = assignedReplicaMap.get(replicaId)
    if (replica == null)
      None
    else
      Some(replica)
  }

  /**
    * 如果当前broker是当前Partition对应的topic-partition副本集合的leader副本
    * 那么就返回该leader对应的Replica实例，否则返回None
    * @return
    */
  def leaderReplicaIfLocal(): Option[Replica] = {
    leaderReplicaIdOpt match {
      case Some(leaderReplicaId) =>
        if (leaderReplicaId == localBrokerId)
          getReplica(localBrokerId)
        else
          None
      case None => None
    }
  }

  def addReplicaIfNotExists(replica: Replica) = {
    assignedReplicaMap.putIfNotExists(replica.brokerId, replica)
  }

  def assignedReplicas(): Set[Replica] = {
    assignedReplicaMap.values.toSet
  }

  def removeReplica(replicaId: Int) {
    assignedReplicaMap.remove(replicaId)
  }

  // 删除当前Partition
  def delete() {
    // need to hold the lock to prevent appendMessagesToLeader() from hitting I/O exceptions due to log being deleted
    inWriteLock(leaderIsrUpdateLock) {
      assignedReplicaMap.clear()
      inSyncReplicas = Set.empty[Replica]
      leaderReplicaIdOpt = None
      try {
        logManager.deleteLog(TopicAndPartition(topic, partitionId))
        removePartitionMetrics()
      } catch {
        case e: IOException =>
          fatal("Error deleting the log for partition [%s,%d]".format(topic, partitionId), e)
          Runtime.getRuntime().halt(1)
      }
    }
  }

  def getLeaderEpoch(): Int = {
    return this.leaderEpoch
  }

  /**
   * Make the local replica the leader by resetting LogEndOffset for remote replicas (there could be old LogEndOffset
   * from the time when this broker was the leader last time) and setting the new leader and ISR.
   * If the leader replica id does not change, return false to indicate the replica manager.
   */
  // 将当前broker对应的topic-partition的Replica设置为leader, 如果leader不变,向ReplicaManager返回false
  def makeLeader(controllerId: Int, partitionStateInfo: PartitionState, correlationId: Int): Boolean = {
    val (leaderHWIncremented, isNewLeader) = inWriteLock(leaderIsrUpdateLock) {
      // 获取topic-partition新的ar副本集合
      val allReplicas: mutable.Set[Int] = partitionStateInfo.replicas.asScala.map(_.toInt)
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path
      // 更新controller epoch
      controllerEpoch = partitionStateInfo.controllerEpoch
      // add replicas that are new
      // 遍历新的副本集合
      // 如果副本的id是当前broker并且对应的Replica还不存在，那么就创建一个Replica实例
      // 如果副本的id是远程的一个broker并且当前broker没有对应的Replica，那么也创建一个
      // 两者区别就是是否创建Log
      allReplicas.foreach(replica => getOrCreateReplica(replica))
      // 获取新的isr列表
      val newInSyncReplicas = partitionStateInfo.isr.asScala.map(r => getOrCreateReplica(r)).toSet
      // remove assigned replicas that have been removed by the controller
      // 将当前AR列表中不在存在的副本删除
      // 如，旧AR{1,2,3},新AR{2,3,4}，那么这个操作就是剔除1
      (assignedReplicas().map(_.brokerId) -- allReplicas).foreach(removeReplica(_))
      // 更新isr和leaderEpoch和zkVersion
      inSyncReplicas = newInSyncReplicas
      leaderEpoch = partitionStateInfo.leaderEpoch
      zkVersion = partitionStateInfo.zkVersion
      // 判断leader是否发生变化
      val isNewLeader =
        // 如果之前是有leader的并且还是当前broker，那么返回false，说明leader前后没有发生变化
        if (leaderReplicaIdOpt.isDefined && leaderReplicaIdOpt.get == localBrokerId) {
          false
        } else {
          leaderReplicaIdOpt = Some(localBrokerId)
          true
        }
      // 获取当前broker的Replica实例对象
      val leaderReplica = getReplica().get
      // we may need to increment high watermark since ISR could be down to 1
      // 如果是新的leader
      if (isNewLeader) {
        // construct the high watermark metadata for the new leader replica
        // 为新的leader replica构建high watermark metadata
        // 设置为当前broker上记录的对应topic-partition的Log日志的HW
        // 这里可能会造成日志丢失,如，之前的leader是其他broker并且上面的HW和LEO分别是80,100
        // 当本broker成为leader后，其记录的HW和LEO，可能只是70,80，这样就丢失了80-100的日志
        leaderReplica.convertHWToLocalOffsetMetadata()
        // reset log end offset for remote replicas
        // 重置当前broker上记录的对应远程broker的Replica的LEO，这里直接清空了远程的LEO
        assignedReplicas.filter(_.brokerId != localBrokerId).foreach(_.updateLogReadResult(LogReadResult.UnknownLogReadResult))
      }
      // 尝试更新HW
      (maybeIncrementLeaderHW(leaderReplica), isNewLeader)
    }
    // some delayed operations may be unblocked after HW changed
    // hw发送了变更，那么此时需要触发一些延迟任务的立即执行
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()
    // 当前broker是否是当前topic-partition新的leader副本
    isNewLeader
  }

  /**
   *  Make the local replica the follower by setting the new leader and ISR to empty
   *  If the leader replica id does not change, return false to indicate the replica manager
   */
  // 将当前broker对应的topic-partition的Replica设置为follower
  def makeFollower(controllerId: Int, partitionStateInfo: PartitionState, correlationId: Int): Boolean = {
    inWriteLock(leaderIsrUpdateLock) {
      val allReplicas = partitionStateInfo.replicas.asScala.map(_.toInt)
      val newLeaderBrokerId: Int = partitionStateInfo.leader
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path
      controllerEpoch = partitionStateInfo.controllerEpoch
      // add replicas that are new
      allReplicas.foreach(r => getOrCreateReplica(r))
      // remove assigned replicas that have been removed by the controller
      (assignedReplicas().map(_.brokerId) -- allReplicas).foreach(removeReplica(_))
      // 设置ISR列表为空
      inSyncReplicas = Set.empty[Replica]
      leaderEpoch = partitionStateInfo.leaderEpoch
      zkVersion = partitionStateInfo.zkVersion
      // leader副本已存在 && leader副本没有发送变化
      if (leaderReplicaIdOpt.isDefined && leaderReplicaIdOpt.get == newLeaderBrokerId) {
        false
      }
      else {
        // leader副本发生了变化
        leaderReplicaIdOpt = Some(newLeaderBrokerId)
        true
      }
    }
  }

  /**
   * Update the log end offset of a certain replica of this partition
    * 更新副本的LEO
   */
  def updateReplicaLogReadResult(replicaId: Int, logReadResult: LogReadResult) {
    getReplica(replicaId) match {
      case Some(replica) =>
        // 更新副本的一些信息
        replica.updateLogReadResult(logReadResult)
        // check if we need to expand ISR to include this replica
        // if it is not in the ISR yet
        // 如果ISR列表没有当前副本，则加入进来，里面会尝试更新leader的HW
        maybeExpandIsr(replicaId)

        debug("Recorded replica %d log end offset (LEO) position %d for partition %s."
          .format(replicaId,
                  logReadResult.info.fetchOffsetMetadata.messageOffset,
                  TopicAndPartition(topic, partitionId)))
      case None =>
        throw new NotAssignedReplicaException(("Leader %d failed to record follower %d's position %d since the replica" +
          " is not recognized to be one of the assigned replicas %s for partition %s.")
          .format(localBrokerId,
                  replicaId,
                  logReadResult.info.fetchOffsetMetadata.messageOffset,
                  assignedReplicas().map(_.brokerId).mkString(","),
                  TopicAndPartition(topic, partitionId)))
    }
  }

  /**
   * Check and maybe expand the ISR of the partition.
   *
   * This function can be triggered when a replica's LEO has incremented
   */
  def maybeExpandIsr(replicaId: Int) {
    val leaderHWIncremented = inWriteLock(leaderIsrUpdateLock) {
      // check if this replica needs to be added to the ISR
      leaderReplicaIfLocal() match {
        case Some(leaderReplica) =>
          // 获取对应的副本
          val replica = getReplica(replicaId).get
          // 获取leader的HW
          val leaderHW = leaderReplica.highWatermark
          // 如果不包含则将当前副本加入到ISR列表
          if(!inSyncReplicas.contains(replica) &&
             assignedReplicas.map(_.brokerId).contains(replicaId) &&
                  replica.logEndOffset.offsetDiff(leaderHW) >= 0) {
            val newInSyncReplicas = inSyncReplicas + replica
            info("Expanding ISR for partition [%s,%d] from %s to %s"
                         .format(topic, partitionId, inSyncReplicas.map(_.brokerId).mkString(","),
                                 newInSyncReplicas.map(_.brokerId).mkString(",")))
            // update ISR in ZK and cache
            // 更新ISR列表到zk
            updateIsr(newInSyncReplicas)
            replicaManager.isrExpandRate.mark()
          }

          // check if the HW of the partition can now be incremented
          // since the replica maybe now be in the ISR and its LEO has just incremented
          // 更新HW
          maybeIncrementLeaderHW(leaderReplica)

        case None => false // nothing to do if no longer leader
      }
    }

    // some delayed operations may be unblocked after HW changed
    // 如果leader副本更新了HW
    if (leaderHWIncremented)
      // 触发一下延迟请求的执行
      tryCompleteDelayedRequests()
  }

  /*
   * Note that this method will only be called if requiredAcks = -1
   * and we are waiting for all replicas in ISR to be fully caught up to
   * the (local) leader's offset corresponding to this produce request
   * before we acknowledge the produce request.
   */
  def checkEnoughReplicasReachOffset(requiredOffset: Long): (Boolean, Short) = {
    leaderReplicaIfLocal() match {
      case Some(leaderReplica) =>
        // keep the current immutable replica list reference
        val curInSyncReplicas = inSyncReplicas
        val numAcks = curInSyncReplicas.count(r => {
          if (!r.isLocal)
            if (r.logEndOffset.messageOffset >= requiredOffset) {
              trace("Replica %d of %s-%d received offset %d".format(r.brokerId, topic, partitionId, requiredOffset))
              true
            }
            else
              false
          else
            true /* also count the local (leader) replica */
        })

        trace("%d acks satisfied for %s-%d with acks = -1".format(numAcks, topic, partitionId))

        val minIsr = leaderReplica.log.get.config.minInSyncReplicas

        if (leaderReplica.highWatermark.messageOffset >= requiredOffset ) {
          /*
          * The topic may be configured not to accept messages if there are not enough replicas in ISR
          * in this scenario the request was already appended locally and then added to the purgatory before the ISR was shrunk
          */
          if (minIsr <= curInSyncReplicas.size) {
            (true, Errors.NONE.code)
          } else {
            (true, Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND.code)
          }
        } else
          (false, Errors.NONE.code)
      case None =>
        (false, Errors.NOT_LEADER_FOR_PARTITION.code)
    }
  }

  /**
   * Check and maybe increment the high watermark of the partition;
   * this function can be triggered when
   * 以下情况会尝试更新HW
   * 1. Partition ISR changed
    *   partiton的ISR发送变动
   * 2. Any replica's LEO changed
    *   某个副本的LEO发生改变，副本fetch请求
   *
   * Returns true if the HW was incremented, and false otherwise.
   * Note There is no need to acquire the leaderIsrUpdate lock here
   * since all callers of this private API acquire that lock
   */
  // 尝试更新当前topic-partition的leader副本的HW
  private def maybeIncrementLeaderHW(leaderReplica: Replica): Boolean = {
    // 获取ISR列表中所有副本的LEO
    val allLogEndOffsets = inSyncReplicas.map(_.logEndOffset)
    // 计算最小的LEO(可能会成为新的HW)
    val newHighWatermark = allLogEndOffsets.min(new LogOffsetMetadata.OffsetOrdering)
    // 获取leader副本上的HW(当前的HW)
    val oldHighWatermark = leaderReplica.highWatermark
    // 判断然后更新leader副本的HW
    if (oldHighWatermark.messageOffset < newHighWatermark.messageOffset || oldHighWatermark.onOlderSegment(newHighWatermark)) {
      leaderReplica.highWatermark = newHighWatermark
      debug("High watermark for partition [%s,%d] updated to %s".format(topic, partitionId, newHighWatermark))
      true
    } else {
      debug("Skipping update high watermark since Old hw %s is larger than new hw %s for partition [%s,%d]. All leo's are %s"
        .format(oldHighWatermark, newHighWatermark, topic, partitionId, allLogEndOffsets.mkString(",")))
      false
    }
  }

  /**
   * Try to complete any pending requests. This should be called without holding the leaderIsrUpdateLock.
   */
  private def tryCompleteDelayedRequests() {
    val requestKey = new TopicPartitionOperationKey(this.topic, this.partitionId)
    replicaManager.tryCompleteDelayedFetch(requestKey)
    replicaManager.tryCompleteDelayedProduce(requestKey)
  }

  // 判断是否需要收缩ISR列表，也就是剔除过期的replica
  // 参数默认10000
  def maybeShrinkIsr(replicaMaxLagTimeMs: Long) {
    val leaderHWIncremented = inWriteLock(leaderIsrUpdateLock) {
      // 先检查当前的broker是否为topic-partition的leader replica
      leaderReplicaIfLocal() match {
        // 是
        case Some(leaderReplica) =>
          // 从分区的ISR列表中过滤出滞后的replica集合
          val outOfSyncReplicas: Set[Replica] = getOutOfSyncReplicas(leaderReplica, replicaMaxLagTimeMs)
          if(outOfSyncReplicas.size > 0) {
            // 计算新的ISR列表
            val newInSyncReplicas: Set[Replica] = inSyncReplicas -- outOfSyncReplicas
            assert(newInSyncReplicas.size > 0)
            info("Shrinking ISR for partition [%s,%d] from %s to %s".format(topic, partitionId,
              inSyncReplicas.map(_.brokerId).mkString(","), newInSyncReplicas.map(_.brokerId).mkString(",")))
            // update ISR in zk and in cache
            // 替换本地ISR列表并更新到zk：/brokers/topics/{topic}/partitions/{partitionId}/state路径下触发相关事件
            // 触发kafka.controller.ReassignedPartitionsIsrChangeListener事件，执行分区重分配
            updateIsr(newInSyncReplicas)
            // we may need to increment high watermark since ISR could be down to 1
            replicaManager.isrShrinkRate.mark()
            // 由于删除了滞后的replicas所以这里判断一下是否需要更新topic-partition的leader副本的HW
            maybeIncrementLeaderHW(leaderReplica)
          } else {
            false
          }
        // 如果是follower什么也不做
        case None => false // do nothing if no longer leader
      }
    }

    // some delayed operations may be unblocked after HW changed
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()
  }

  /**
    * 从当前分区的ISR列表集合中获取滞后的副本实例集合
    * @param leaderReplica 当前分区的leader副本实例
    * @param maxLagMs 最大延迟时间，默认10000L
    * @return
    */
  def getOutOfSyncReplicas(leaderReplica: Replica, maxLagMs: Long): Set[Replica] = {
    /**
     * there are two cases that will be handled here -
      * 只处理以下两种情况
     * 1. Stuck followers: If the leo of the replica hasn't been updated for maxLagMs ms,
     *                     the follower is stuck and should be removed from the ISR
      *                     如果超过maxLagMs(10s),副本的LEO还没有更新(还没有发起fetch请求),则认为follower卡主了
     * 2. Slow followers: If the replica has not read up to the leo within the last maxLagMs ms,
     *                    then the follower is lagging and should be removed from the ISR
      *                     如果超过maxLagMs(10s),副本没有更新到最新的LEO，则认为follower太慢了
     * Both these cases are handled by checking the lastCaughtUpTimeMs which represents
     * the last time when the replica was fully caught up. If either of the above conditions
     * is violated, that replica is considered to be out of sync
     *
     **/
    // 获取leader副本的LEO
    val leaderLogEndOffset = leaderReplica.logEndOffset
    // 获取ISR列表中除去leader副本，返回候选副本集合candidateReplicas(不需要处理leader副本，它是参考对象)
    val candidateReplicas: Set[Replica] = inSyncReplicas - leaderReplica
    // 遍历候选副本集合candidateReplicas
    // 过滤出副本属性lastCaughtUpTimeMs距离当前时间已经超过maxLagMs的副本
    val laggingReplicas: Set[Replica] = candidateReplicas.filter(r => (time.milliseconds - r.lastCaughtUpTimeMs) > maxLagMs)
    if(laggingReplicas.size > 0)
      debug("Lagging replicas for partition %s are %s".format(TopicAndPartition(topic, partitionId), laggingReplicas.map(_.brokerId).mkString(",")))
    // 返回滞后的副本集合
    laggingReplicas
  }

  /**
    * 追加消息
    * @param messages 消息集合
    * @param requiredAcks acks
    * @return
    */
  def appendMessagesToLeader(messages: ByteBufferMessageSet, requiredAcks: Int = 0) = {
    val (info, leaderHWIncremented) = inReadLock(leaderIsrUpdateLock) {
      // 获取当前broker上对应topic-partition的leader副本
      val leaderReplicaOpt = leaderReplicaIfLocal()
      leaderReplicaOpt match {
        case Some(leaderReplica) =>
          // 获取leader副本的Log对象
          val log = leaderReplica.log.get
          // 获取配置min.insync.replicas，默认1以及当前的ISR列表数量
          val minIsr = log.config.minInSyncReplicas
          val inSyncSize = inSyncReplicas.size

          // Avoid writing to leader if there are not enough insync replicas to make it safe
          // 如果ISR列表数量不满足minIsr配置并且写入消息的acks配置为-1(all)时，抛出异常
          if (inSyncSize < minIsr && requiredAcks == -1) {
            throw new NotEnoughReplicasException("Number of insync replicas for partition [%s,%d] is [%d], below required minimum [%d]"
              .format(topic, partitionId, inSyncSize, minIsr))
          }

          // 写入消息到Log(注意这里分配offset为true)
          // 返回info(消息集合中第一条消息的offset, 消息集合中最后一条消息的offset，消息集合中消息压缩类型，服务端消息压缩类型，验证通过的消息总数，验证通过的消息总字节数，消息集合中消息是否单调递增)
          val info: LogAppendInfo = log.append(messages, assignOffsets = true)
          // probably unblock some follower fetch requests since log end offset has been updated
          // 可能解除一些追随者获取请求，因为日志结束偏移量已经更新
          // 唤醒在时间轮中等待某个分区数据的fetch任务
          replicaManager.tryCompleteDelayedFetch(new TopicPartitionOperationKey(this.topic, this.partitionId))
          // we may need to increment high watermark since ISR could be down to 1
          // 返回拼接日志的结果以及是否更新了HW
          (info, maybeIncrementLeaderHW(leaderReplica))
        // 不存Log则抛出异常
        case None =>
          throw new NotLeaderForPartitionException("Leader not local for partition [%s,%d] on broker %d"
            .format(topic, partitionId, localBrokerId))
      }
    }

    // some delayed operations may be unblocked after HW changed
    // 如果更新了Leader副本的HW，那么去唤醒时间轮中的一些等待请求
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()

    info
  }

  /**
    * 更新当前分区的ISR列表集合
    * @param newIsr
    */
  private def updateIsr(newIsr: Set[Replica]) {
    val newLeaderAndIsr = new LeaderAndIsr(localBrokerId, leaderEpoch, newIsr.map(r => r.brokerId).toList, zkVersion)
    // 更新zk上的节点 ：/brokers/topics/{topic}/partitions/{partitionId}/state
    // 触发kafka.controller.ReassignedPartitionsIsrChangeListener事件，执行分区重分配
    val (updateSucceeded,newVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, topic, partitionId,
      newLeaderAndIsr, controllerEpoch, zkVersion)
    // 更新成功
    if(updateSucceeded) {
      // 将变更了ISR的topic-partition记录到ReplicaManager的isrChangeSet集合中
      replicaManager.recordIsrChange(new TopicAndPartition(topic, partitionId))
      // 更新当前分区ISR列表
      inSyncReplicas = newIsr
      // 更新当前分区ISR列表数据在zk上的Version
      zkVersion = newVersion
      trace("ISR updated to [%s] and zkVersion updated to [%d]".format(newIsr.mkString(","), zkVersion))
    } else {
      info("Cached zkVersion [%d] not equal to that in zookeeper, skip updating ISR".format(zkVersion))
    }
  }

  /**
   * remove deleted log metrics
   */
  private def removePartitionMetrics() {
    removeMetric("UnderReplicated", tags)
  }

  override def equals(that: Any): Boolean = {
    if(!(that.isInstanceOf[Partition]))
      return false
    val other = that.asInstanceOf[Partition]
    if(topic.equals(other.topic) && partitionId == other.partitionId)
      return true
    false
  }

  override def hashCode(): Int = {
    31 + topic.hashCode() + 17*partitionId
  }

  override def toString(): String = {
    val partitionString = new StringBuilder
    partitionString.append("Topic: " + topic)
    partitionString.append("; Partition: " + partitionId)
    partitionString.append("; Leader: " + leaderReplicaIdOpt)
    partitionString.append("; AssignedReplicas: " + assignedReplicaMap.keys.mkString(","))
    partitionString.append("; InSyncReplicas: " + inSyncReplicas.map(_.brokerId).mkString(","))
    partitionString.toString()
  }
}
