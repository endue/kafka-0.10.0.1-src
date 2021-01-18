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

import java.io.{File, IOException}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import com.yammer.metrics.core.Gauge
import kafka.api._
import kafka.cluster.{Partition, Replica}
import kafka.common._
import kafka.controller.KafkaController
import kafka.log.{LogAppendInfo, LogManager}
import kafka.message.{ByteBufferMessageSet, InvalidMessageException, Message, MessageSet}
import kafka.metrics.KafkaMetricsGroup
import kafka.utils._
import org.apache.kafka.common.errors.{ControllerMovedException, CorruptRecordException, InvalidTimestampException,
                                        InvalidTopicException, NotLeaderForPartitionException, OffsetOutOfRangeException,
                                        RecordBatchTooLargeException, RecordTooLargeException, ReplicaNotAvailableException,
                                        UnknownTopicOrPartitionException}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{LeaderAndIsrRequest, PartitionState, StopReplicaRequest, UpdateMetadataRequest}
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.utils.{Time => JTime}

import scala.collection._
import scala.collection.JavaConverters._

/*
 * Result metadata of a log append operation on the log
 */
case class LogAppendResult(info: LogAppendInfo, error: Option[Throwable] = None) {
  def errorCode = error match {
    case None => Errors.NONE.code
    case Some(e) => Errors.forException(e).code
  }
}

/*
 * Result metadata of a log read operation on the log
 * @param info @FetchDataInfo returned by the @Log read
 * @param hw high watermark of the local replica
 * @param readSize amount of data that was read from the log i.e. size of the fetch
 * @param isReadFromLogEnd true if the request read up to the log end offset snapshot
 *                         when the read was initiated, false otherwise
 * @param error Exception if error encountered while reading from the log
 */
case class LogReadResult(info: FetchDataInfo,
                         hw: Long,
                         readSize: Int,
                         isReadFromLogEnd : Boolean,
                         error: Option[Throwable] = None) {

  def errorCode = error match {
    case None => Errors.NONE.code
    case Some(e) => Errors.forException(e).code
  }

  override def toString = {
    "Fetch Data: [%s], HW: [%d], readSize: [%d], isReadFromLogEnd: [%b], error: [%s]"
            .format(info, hw, readSize, isReadFromLogEnd, error)
  }
}

object LogReadResult {
  val UnknownLogReadResult = LogReadResult(FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata,
                                                         MessageSet.Empty),
                                           -1L,
                                           -1,
                                           false)
}

case class BecomeLeaderOrFollowerResult(responseMap: collection.Map[TopicPartition, Short], errorCode: Short) {

  override def toString = {
    "update results: [%s], global error: [%d]".format(responseMap, errorCode)
  }
}

object ReplicaManager {
  val HighWatermarkFilename = "replication-offset-checkpoint"
  val IsrChangePropagationBlackOut = 5000L
  val IsrChangePropagationInterval = 60000L
}

class ReplicaManager(val config: KafkaConfig,
                     metrics: Metrics,
                     time: Time,
                     jTime: JTime,
                     val zkUtils: ZkUtils,
                     scheduler: Scheduler,
                     val logManager: LogManager,
                     val isShuttingDown: AtomicBoolean,
                     threadNamePrefix: Option[String] = None) extends Logging with KafkaMetricsGroup {
  /* epoch of the controller that last changed the leader */
  // 记录leader发生变化时controller的epoch,epoch存储在zk中的/Controller_epoch中
  @volatile var controllerEpoch: Int = KafkaController.InitialControllerEpoch - 1
  // 记录当前broker的ID
  private val localBrokerId = config.brokerId
  // 保存所有的topic <--> partition映射关系
  // 记录了某主题的某分区的相关信息
  private val allPartitions = new Pool[(String, Int), Partition](valueFactory = Some { case (t, p) =>
    new Partition(t, p, time, this)
  })
  private val replicaStateChangeLock = new Object
  // 分区成为follow后，就创建一个线程开始复制消息，这个线程会不断往leader发送FETCH请求拉取数据
  val replicaFetcherManager = new ReplicaFetcherManager(config, this, metrics, jTime, threadNamePrefix)
  // HW相关
  private val highWatermarkCheckPointThreadStarted = new AtomicBoolean(false)
  // replication-offset-checkpoint文件，记录各个topic-partition的hw
  val highWatermarkCheckpoints = config.logDirs.map(dir => (new File(dir).getAbsolutePath, new OffsetCheckpoint(new File(dir, ReplicaManager.HighWatermarkFilename)))).toMap
  private var hwThreadInitialized = false
  this.logIdent = "[Replica Manager on Broker " + localBrokerId + "]: "
  val stateChangeLogger = KafkaController.stateChangeLogger
  // ISR变更列表，记录某个isr发生变更的topic-partition
  private val isrChangeSet: mutable.Set[TopicAndPartition] = new mutable.HashSet[TopicAndPartition]()
  // ISR列表最后变更时间戳
  private val lastIsrChangeMs = new AtomicLong(System.currentTimeMillis())
  // ISR列表最后发布时间戳
  private val lastIsrPropagationMs = new AtomicLong(System.currentTimeMillis())
  // 延迟调度，封装了producer的回调
  val delayedProducePurgatory = DelayedOperationPurgatory[DelayedProduce](
    purgatoryName = "Produce", config.brokerId, config.producerPurgatoryPurgeIntervalRequests)
  // 延时调度，封装了fetch请求
  val delayedFetchPurgatory = DelayedOperationPurgatory[DelayedFetch](
    purgatoryName = "Fetch", config.brokerId, config.fetchPurgatoryPurgeIntervalRequests)
  // 记录本地存储了多少leader
  val leaderCount = newGauge(
    "LeaderCount",
    new Gauge[Int] {
      def value = {
          getLeaderPartitions().size
      }
    }
  )
  // 记录本地存储了多少partition
  val partitionCount = newGauge(
    "PartitionCount",
    new Gauge[Int] {
      def value = allPartitions.size
    }
  )
  // 记录副本数量不足的partition
  val underReplicatedPartitions = newGauge(
    "UnderReplicatedPartitions",
    new Gauge[Int] {
      def value = underReplicatedPartitionCount()
    }
  )
  // ISR列表扩容速率
  val isrExpandRate = newMeter("IsrExpandsPerSec",  "expands", TimeUnit.SECONDS)
  val isrShrinkRate = newMeter("IsrShrinksPerSec",  "shrinks", TimeUnit.SECONDS)

  // 获取ISR != AR的topic-partition数量
  def underReplicatedPartitionCount(): Int = {
      getLeaderPartitions().count(_.isUnderReplicated)
  }

  // 定时任务,定时处理各个topic-partition的hw
  // replica.high.watermark.checkpoint.interval.ms默认5000L
  def startHighWaterMarksCheckPointThread() = {
    if(highWatermarkCheckPointThreadStarted.compareAndSet(false, true))
      scheduler.schedule("highwatermark-checkpoint", checkpointHighWatermarks, period = config.replicaHighWatermarkCheckpointIntervalMs, unit = TimeUnit.MILLISECONDS)
  }

  /**
    * 记录变更的TopicAndPartition
    * @param topicAndPartition
    */
  def recordIsrChange(topicAndPartition: TopicAndPartition) {
    isrChangeSet synchronized {
      // 记录到isrChangeSet中
      isrChangeSet += topicAndPartition
      lastIsrChangeMs.set(System.currentTimeMillis())
    }
  }
  /**
   * This function periodically runs to see if ISR needs to be propagated. It propagates ISR when:
   * 1. There is ISR change not propagated yet.
    *   ISR列表发生变更但还没有被广播出去
   * 2. There is no ISR Change in the last five seconds, or it has been more than 60 seconds since the last ISR propagation.
    *   在最近5秒内没有ISR变化，或者自上次ISR广播以来已经超过60秒
   * This allows an occasional ISR change to be propagated within a few seconds, and avoids overwhelming controller and
   * other brokers when large amount of ISR change occurs.
   */
  def maybePropagateIsrChanges() {
    val now = System.currentTimeMillis()
    isrChangeSet synchronized {
      // isrChangeSet列表不为空 && (isrChangeSet最近5s没有变化 || isrChangeSet自上次传播后已过去60s )
      if (isrChangeSet.nonEmpty && // isrChangeSet集合列表不为空
        (lastIsrChangeMs.get() + ReplicaManager.IsrChangePropagationBlackOut < now || // 最近5秒内isrChangeSet列表没有变化
          lastIsrPropagationMs.get() + ReplicaManager.IsrChangePropagationInterval < now)) { // 自上次ISR传播以来已经超过60秒
        // 将集合列表中的数据，发布到zk ： /isr_change_notification/isr_change_{序列号isrChangeSet} 上
        ReplicationUtils.propagateIsrChanges(zkUtils, isrChangeSet)
        // 清空isrChangeSet集合列表
        isrChangeSet.clear()
        // 更新最后传播时间
        lastIsrPropagationMs.set(now)
      }
    }
  }

  /**
   * Try to complete some delayed produce requests with the request key;
   * this can be triggered when:
   *
   * 1. The partition HW has changed (for acks = -1)
   * 2. A follower replica's fetch operation is received (for acks > 1)
   */
  // 尝试执行producer的回调请求
  def tryCompleteDelayedProduce(key: DelayedOperationKey) {
    val completed = delayedProducePurgatory.checkAndComplete(key)
    debug("Request key %s unblocked %d producer requests.".format(key.keyLabel, completed))
  }

  /**
   * Try to complete some delayed fetch requests with the request key;
   * this can be triggered when:
   *
   * 1. The partition HW has changed (for regular fetch)
   * 2. A new message set is appended to the local log (for follower fetch)
   */
  def tryCompleteDelayedFetch(key: DelayedOperationKey) {
    val completed = delayedFetchPurgatory.checkAndComplete(key)
    debug("Request key %s unblocked %d fetch requests.".format(key.keyLabel, completed))
  }

  // 启动两个线程，维护ISR列表相关
  def startup() {
    // start ISR expiration thread 启动ISR过期线程
    // 周期性检查ISRL列表是否有replica过期需要从ISRL列表中移除，配置replica.lag.time.max.ms，默认10000L，也就是10000ms
    scheduler.schedule("isr-expiration", maybeShrinkIsr, period = config.replicaLagTimeMaxMs, unit = TimeUnit.MILLISECONDS)
    // 周期性检查是不是有topic-partition的isr需要变动,如果需要,就更新到zk上,来触发controller，执行时间写死，2500ms执行一次
    scheduler.schedule("isr-change-propagation", maybePropagateIsrChanges, period = 2500L, unit = TimeUnit.MILLISECONDS)
  }

  def stopReplica(topic: String, partitionId: Int, deletePartition: Boolean): Short  = {
    stateChangeLogger.trace("Broker %d handling stop replica (delete=%s) for partition [%s,%d]".format(localBrokerId,
      deletePartition.toString, topic, partitionId))
    val errorCode = Errors.NONE.code
    getPartition(topic, partitionId) match {
      case Some(partition) =>
        if(deletePartition) {
          val removedPartition = allPartitions.remove((topic, partitionId))
          if (removedPartition != null) {
            removedPartition.delete() // this will delete the local log
            val topicHasPartitions = allPartitions.keys.exists { case (t, _) => topic == t }
            if (!topicHasPartitions)
                BrokerTopicStats.removeMetrics(topic)
          }
        }
      case None =>
        // Delete log and corresponding folders in case replica manager doesn't hold them anymore.
        // This could happen when topic is being deleted while broker is down and recovers.
        if(deletePartition) {
          val topicAndPartition = TopicAndPartition(topic, partitionId)

          if(logManager.getLog(topicAndPartition).isDefined) {
              logManager.deleteLog(topicAndPartition)
          }
        }
        stateChangeLogger.trace("Broker %d ignoring stop replica (delete=%s) for partition [%s,%d] as replica doesn't exist on broker"
          .format(localBrokerId, deletePartition, topic, partitionId))
    }
    stateChangeLogger.trace("Broker %d finished handling stop replica (delete=%s) for partition [%s,%d]"
      .format(localBrokerId, deletePartition, topic, partitionId))
    errorCode
  }

  def stopReplicas(stopReplicaRequest: StopReplicaRequest): (mutable.Map[TopicPartition, Short], Short) = {
    replicaStateChangeLock synchronized {
      val responseMap = new collection.mutable.HashMap[TopicPartition, Short]
      if(stopReplicaRequest.controllerEpoch() < controllerEpoch) {
        stateChangeLogger.warn("Broker %d received stop replica request from an old controller epoch %d. Latest known controller epoch is %d"
          .format(localBrokerId, stopReplicaRequest.controllerEpoch, controllerEpoch))
        (responseMap, Errors.STALE_CONTROLLER_EPOCH.code)
      } else {
        val partitions = stopReplicaRequest.partitions.asScala
        controllerEpoch = stopReplicaRequest.controllerEpoch
        // First stop fetchers for all partitions, then stop the corresponding replicas
        replicaFetcherManager.removeFetcherForPartitions(partitions.map(r => TopicAndPartition(r.topic, r.partition)))
        for(topicPartition <- partitions){
          val errorCode = stopReplica(topicPartition.topic, topicPartition.partition, stopReplicaRequest.deletePartitions)
          responseMap.put(topicPartition, errorCode)
        }
        (responseMap, Errors.NONE.code)
      }
    }
  }

  def getOrCreatePartition(topic: String, partitionId: Int): Partition = {
    allPartitions.getAndMaybePut((topic, partitionId))
  }

  /**
    * 获取某个主题下某个分区号对应的分区元数据
    * @param topic 主题
    * @param partitionId 分区号
    * @return
    */
  def getPartition(topic: String, partitionId: Int): Option[Partition] = {
    val partition = allPartitions.get((topic, partitionId))
    if (partition == null)
      None
    else
      Some(partition)
  }

  def getReplicaOrException(topic: String, partition: Int): Replica = {
    val replicaOpt = getReplica(topic, partition)
    if(replicaOpt.isDefined)
      replicaOpt.get
    else
      throw new ReplicaNotAvailableException("Replica %d is not available for partition [%s,%d]".format(config.brokerId, topic, partition))
  }

  def getLeaderReplicaIfLocal(topic: String, partitionId: Int): Replica =  {
    val partitionOpt = getPartition(topic, partitionId)
    partitionOpt match {
      case None =>
        throw new UnknownTopicOrPartitionException("Partition [%s,%d] doesn't exist on %d".format(topic, partitionId, config.brokerId))
      case Some(partition) =>
        partition.leaderReplicaIfLocal match {
          case Some(leaderReplica) => leaderReplica
          case None =>
            throw new NotLeaderForPartitionException("Leader not local for partition [%s,%d] on broker %d"
                                                     .format(topic, partitionId, config.brokerId))
        }
    }
  }

  def getReplica(topic: String, partitionId: Int, replicaId: Int = config.brokerId): Option[Replica] =  {
    val partitionOpt = getPartition(topic, partitionId)
    partitionOpt match {
      case None => None
      case Some(partition) => partition.getReplica(replicaId)
    }
  }

  /**
    * 将消息追加到分区的leader副本同时等待赋值到其他副本
    * 当超时或所需的acks得到满足时，将触发回调函数
   * Append messages to leader replicas of the partition, and wait for them to be replicated to other replicas;
   * the callback function will be triggered either when timeout or the required acks are satisfied
   */
  def appendMessages(timeout: Long,
                     requiredAcks: Short,
                     internalTopicsAllowed: Boolean,
                     messagesPerPartition: Map[TopicPartition, MessageSet],
                     responseCallback: Map[TopicPartition, PartitionResponse] => Unit) {
    // 验证acks配置
    if (isValidRequiredAcks(requiredAcks)) {
      val sTime = SystemTime.milliseconds
      // 添加消息到本地文件
      val localProduceResults = appendToLocalLog(internalTopicsAllowed, messagesPerPartition, requiredAcks)
      debug("Produce to local log in %d ms".format(SystemTime.milliseconds - sTime))

      val produceStatus = localProduceResults.map { case (topicPartition, result) =>
        topicPartition ->
                ProducePartitionStatus(
                  result.info.lastOffset + 1, // required offset
                  new PartitionResponse(result.errorCode, result.info.firstOffset, result.info.timestamp)) // response status
      }

      if (delayedRequestRequired(requiredAcks, messagesPerPartition, localProduceResults)) {
        // create delayed produce operation
        val produceMetadata = ProduceMetadata(requiredAcks, produceStatus)
        val delayedProduce = new DelayedProduce(timeout, produceMetadata, this, responseCallback)

        // create a list of (topic, partition) pairs to use as keys for this delayed produce operation
        val producerRequestKeys = messagesPerPartition.keys.map(new TopicPartitionOperationKey(_)).toSeq

        // try to complete the request immediately, otherwise put it into the purgatory
        // this is because while the delayed produce operation is being created, new
        // requests may arrive and hence make this operation completable.
        // 如果acks = all，则等待消息同步到其他副本，这里封装成一个任务，添加到时间轮中
        delayedProducePurgatory.tryCompleteElseWatch(delayedProduce, producerRequestKeys)

      } else {
        // we can respond immediately
        val produceResponseStatus = produceStatus.mapValues(status => status.responseStatus)
        responseCallback(produceResponseStatus)
      }
    } else {
      // If required.acks is outside accepted range, something is wrong with the client
      // Just return an error and don't handle the request at all
      val responseStatus = messagesPerPartition.map {
        case (topicAndPartition, messageSet) =>
          (topicAndPartition -> new PartitionResponse(Errors.INVALID_REQUIRED_ACKS.code,
                                                      LogAppendInfo.UnknownLogAppendInfo.firstOffset,
                                                      Message.NoTimestamp))
      }
      responseCallback(responseStatus)
    }
  }

  // If all the following conditions are true, we need to put a delayed produce request and wait for replication to complete
  //
  // 1. required acks = -1
  // 2. there is data to append
  // 3. at least one partition append was successful (fewer errors than partitions)
  private def delayedRequestRequired(requiredAcks: Short, messagesPerPartition: Map[TopicPartition, MessageSet],
                                       localProduceResults: Map[TopicPartition, LogAppendResult]): Boolean = {
    requiredAcks == -1 &&
    messagesPerPartition.size > 0 &&
    localProduceResults.values.count(_.error.isDefined) < messagesPerPartition.size
  }

  private def isValidRequiredAcks(requiredAcks: Short): Boolean = {
    requiredAcks == -1 || requiredAcks == 1 || requiredAcks == 0
  }

  /**
   * Append the messages to the local replica logs
    * 将消息追加到本地副本日志文件
   */
  private def appendToLocalLog(internalTopicsAllowed: Boolean,
                               messagesPerPartition: Map[TopicPartition, MessageSet],
                               requiredAcks: Short): Map[TopicPartition, LogAppendResult] = {
    trace("Append [%s] to local log ".format(messagesPerPartition))
    messagesPerPartition.map { case (topicPartition, messages) =>
      BrokerTopicStats.getBrokerTopicStats(topicPartition.topic).totalProduceRequestRate.mark()
      BrokerTopicStats.getBrokerAllTopicsStats().totalProduceRequestRate.mark()

      // reject appending to internal topics if it is not allowed
      // 验证是否将消息添加到内部队列并且还不允许添加，那么需要返回给客户端异常
      if (Topic.isInternal(topicPartition.topic) && !internalTopicsAllowed) {
        (topicPartition, LogAppendResult(
          LogAppendInfo.UnknownLogAppendInfo,
          Some(new InvalidTopicException("Cannot append to internal topic %s".format(topicPartition.topic)))))
      } else {
        try {
          // 基于主题和分区号获取对应的Partition对象
          val partitionOpt = getPartition(topicPartition.topic, topicPartition.partition)
          val info = partitionOpt match {
            case Some(partition) =>
              // 拼接消息
              partition.appendMessagesToLeader(messages.asInstanceOf[ByteBufferMessageSet], requiredAcks)
            case None => throw new UnknownTopicOrPartitionException("Partition %s doesn't exist on %d"
              .format(topicPartition, localBrokerId))
          }

          val numAppendedMessages =
            if (info.firstOffset == -1L || info.lastOffset == -1L)
              0
            else
              info.lastOffset - info.firstOffset + 1

          // update stats for successfully appended bytes and messages as bytesInRate and messageInRate
          BrokerTopicStats.getBrokerTopicStats(topicPartition.topic).bytesInRate.mark(messages.sizeInBytes)
          BrokerTopicStats.getBrokerAllTopicsStats.bytesInRate.mark(messages.sizeInBytes)
          BrokerTopicStats.getBrokerTopicStats(topicPartition.topic).messagesInRate.mark(numAppendedMessages)
          BrokerTopicStats.getBrokerAllTopicsStats.messagesInRate.mark(numAppendedMessages)

          trace("%d bytes written to log %s-%d beginning at offset %d and ending at offset %d"
            .format(messages.sizeInBytes, topicPartition.topic, topicPartition.partition, info.firstOffset, info.lastOffset))
          (topicPartition, LogAppendResult(info))
        } catch {
          // NOTE: Failed produce requests metric is not incremented for known exceptions
          // it is supposed to indicate un-expected failures of a broker in handling a produce request
          case e: KafkaStorageException =>
            fatal("Halting due to unrecoverable I/O error while handling produce request: ", e)
            Runtime.getRuntime.halt(1)
            (topicPartition, null)
          case e@ (_: UnknownTopicOrPartitionException |
                   _: NotLeaderForPartitionException |
                   _: RecordTooLargeException |
                   _: RecordBatchTooLargeException |
                   _: CorruptRecordException |
                   _: InvalidMessageException |
                   _: InvalidTimestampException) =>
            (topicPartition, LogAppendResult(LogAppendInfo.UnknownLogAppendInfo, Some(e)))
          case t: Throwable =>
            BrokerTopicStats.getBrokerTopicStats(topicPartition.topic).failedProduceRequestRate.mark()
            BrokerTopicStats.getBrokerAllTopicsStats.failedProduceRequestRate.mark()
            error("Error processing append operation on partition %s".format(topicPartition), t)
            (topicPartition, LogAppendResult(LogAppendInfo.UnknownLogAppendInfo, Some(t)))
        }
      }
    }
  }

  /**
   * Fetch messages from the leader replica, and wait until enough data can be fetched and return;
    * 从leader replica中获取消息，并等待足够的数据被获取并返回
   * the callback function will be triggered either when timeout or required fetch info is satisfied
    * 回调函数将在超时或所需的获取信息满足时被触发
   */
  // 处理ApiKeys.FETCH请求
  // 从 leader 拉取数据,等待拉取到足够的数据或者达到 timeout 时间后返回拉取的结果
  def fetchMessages(timeout: Long,// 超时时间
                    replicaId: Int,// 副本ID
                    fetchMinBytes: Int,// 拉取的最小字节数
                    fetchInfo: immutable.Map[TopicAndPartition, PartitionFetchInfo],// 拉取的一些信息
                    responseCallback: Map[TopicAndPartition, FetchResponsePartitionData] => Unit) {// 回调
    // 判断请求是来自follower还是consumer(-1)
    val isFromFollower = replicaId >= 0
    // 是否从leader副本拉取消息
    val fetchOnlyFromLeader: Boolean = replicaId != Request.DebuggingConsumerId
    // 是否只拉取committed的消息
    // 如果拉取请求来自 consumer（true）,只拉取 HW 以内的数据,如果是来自Replica同步,则没有该限制（false）
    val fetchOnlyCommitted: Boolean = ! Request.isValidBrokerId(replicaId)

    // read from local logs
    // 从副本拉取消息日志返回给follower
    val logReadResults = readFromLocalLog(fetchOnlyFromLeader, fetchOnlyCommitted, fetchInfo)

    // if the fetch comes from the follower,
    // update its corresponding log end offset
    // 如果请求是从follow发出的，则更新本地记录的follower副本的LEO，并推进HW
    if(Request.isValidBrokerId(replicaId))
      // 这里也是重点
      updateFollowerLogReadResults(replicaId, logReadResults)

    // check if this fetch request can be satisfied right away
    // 获取读取的字节数
    val bytesReadable = logReadResults.values.map(_.info.messageSet.sizeInBytes).sum
    val errorReadingData = logReadResults.values.foldLeft(false) ((errorIncurred, readResult) =>
      errorIncurred || (readResult.errorCode != Errors.NONE.code))

    // respond immediately if 1) fetch request does not want to wait  fetch请求不需要等待，等待超时了
    //                        2) fetch request does not require any data fetch请求不需要任何数据，拉取结果为空
    //                        3) has enough data to respond 拉取到足够的数据
    //                        4) some error happens while reading data 读取消息遇到异常
    // 超时 || 拉取消息为空 || 拉取到足够的数据 || 读取消息异常
    if(timeout <= 0 || fetchInfo.size <= 0 || bytesReadable >= fetchMinBytes || errorReadingData) {
      // 构建返回的数据
      val fetchPartitionData = logReadResults.mapValues(result =>
        // 返回数据中记录当前副本的HW
        FetchResponsePartitionData(result.errorCode, result.hw, result.info.messageSet))
      // 执行回调方法并将数据返回
      responseCallback(fetchPartitionData)
    // 没有数据，等待
    } else {
      // construct the fetch results from the read results
      // 其他情况，构建延迟发送结果
      val fetchPartitionStatus = logReadResults.map { case (topicAndPartition, result) =>
        (topicAndPartition, FetchPartitionStatus(result.info.fetchOffsetMetadata, fetchInfo.get(topicAndPartition).get))
      }
      val fetchMetadata = FetchMetadata(fetchMinBytes, fetchOnlyFromLeader, fetchOnlyCommitted, isFromFollower, fetchPartitionStatus)
      val delayedFetch = new DelayedFetch(timeout, fetchMetadata, this, responseCallback)

      // create a list of (topic, partition) pairs to use as keys for this delayed fetch operation
      val delayedFetchKeys = fetchPartitionStatus.keys.map(new TopicPartitionOperationKey(_)).toSeq

      // try to complete the request immediately, otherwise put it into the purgatory;
      // this is because while the delayed fetch operation is being created, new requests
      // may arrive and hence make this operation completable.
      // 封装fetch请求到时间轮中
      delayedFetchPurgatory.tryCompleteElseWatch(delayedFetch, delayedFetchKeys)
    }
  }

  /**
   * Read from a single topic/partition at the given offset upto maxSize bytes
    * 从单个主题/分区中以给定偏移量读取最大maxSize字节
   */
  // 从副本拉取消息
  def readFromLocalLog(fetchOnlyFromLeader: Boolean,
                       readOnlyCommitted: Boolean,
                       readPartitionInfo: Map[TopicAndPartition, PartitionFetchInfo]): Map[TopicAndPartition, LogReadResult] = {

    readPartitionInfo.map { case (TopicAndPartition(topic, partition), PartitionFetchInfo(offset, fetchSize)) =>
      BrokerTopicStats.getBrokerTopicStats(topic).totalFetchRequestRate.mark()
      BrokerTopicStats.getBrokerAllTopicsStats().totalFetchRequestRate.mark()

      val partitionDataAndOffsetInfo =
        try {
          trace("Fetching log segment for topic %s, partition %d, offset %d, size %d".format(topic, partition, offset, fetchSize))

          // decide whether to only fetch from leader
          // 获取要从哪个副本拉取消息并决定是否只从leader拉取
          val localReplica = if (fetchOnlyFromLeader)
            // 获取leader副本的Replica
            getLeaderReplicaIfLocal(topic, partition)
          else
            // 获取本机副本的Replica
            getReplicaOrException(topic, partition)

          // decide whether to only fetch committed data (i.e. messages below high watermark)
          // 获取要拉取消息的最大偏移量并决定是否只获取已提交的数据
          // 如果是consumer拉取消息那就是HW，如果是其他副本那就没有限制
          val maxOffsetOpt = if (readOnlyCommitted)
            Some(localReplica.highWatermark.messageOffset)
          else
            None

          /* Read the LogOffsetMetadata prior to performing the read from the log.
           * We use the LogOffsetMetadata to determine if a particular replica is in-sync or not.
           * Using the log end offset after performing the read can lead to a race condition
           * where data gets appended to the log immediately after the replica has consumed from it
           * This can cause a replica to always be out of sync.
           */
          // 获取被拉取消息副本的logEndOffsetMetadata，里面记录了LEO
          val initialLogEndOffset = localReplica.logEndOffset
          // 读取消息
          val logReadInfo = localReplica.log match {
            case Some(log) =>
              log.read(offset, fetchSize, maxOffsetOpt)
            case None =>
              error("Leader for partition [%s,%d] does not have a local log".format(topic, partition))
              FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MessageSet.Empty)
          }
          // 被拉取消息副本的LEO - 被拉取消息的startOffset <= 0 如果成立，那么说明已经没有消息可拉取了
          val readToEndOfLog = initialLogEndOffset.messageOffset - logReadInfo.fetchOffsetMetadata.messageOffset <= 0
          // 封装一个LogReadResult返回
          LogReadResult(logReadInfo, localReplica.highWatermark.messageOffset, fetchSize, readToEndOfLog, None)
        } catch {
          // NOTE: Failed fetch requests metric is not incremented for known exceptions since it
          // is supposed to indicate un-expected failure of a broker in handling a fetch request
          case utpe: UnknownTopicOrPartitionException =>
            LogReadResult(FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MessageSet.Empty), -1L, fetchSize, false, Some(utpe))
          case nle: NotLeaderForPartitionException =>
            LogReadResult(FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MessageSet.Empty), -1L, fetchSize, false, Some(nle))
          case rnae: ReplicaNotAvailableException =>
            LogReadResult(FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MessageSet.Empty), -1L, fetchSize, false, Some(rnae))
          case oor : OffsetOutOfRangeException =>
            LogReadResult(FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MessageSet.Empty), -1L, fetchSize, false, Some(oor))
          case e: Throwable =>
            BrokerTopicStats.getBrokerTopicStats(topic).failedFetchRequestRate.mark()
            BrokerTopicStats.getBrokerAllTopicsStats().failedFetchRequestRate.mark()
            error("Error processing fetch operation on partition [%s,%d] offset %d".format(topic, partition, offset), e)
            LogReadResult(FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MessageSet.Empty), -1L, fetchSize, false, Some(e))
        }
      (TopicAndPartition(topic, partition), partitionDataAndOffsetInfo)
    }
  }

  def getMessageFormatVersion(topicAndPartition: TopicAndPartition): Option[Byte] =
    getReplica(topicAndPartition.topic, topicAndPartition.partition).flatMap { replica =>
      replica.log.map(_.config.messageFormatVersion.messageFormatVersion)
    }

  def maybeUpdateMetadataCache(correlationId: Int, updateMetadataRequest: UpdateMetadataRequest, metadataCache: MetadataCache) {
    replicaStateChangeLock synchronized {
      if(updateMetadataRequest.controllerEpoch < controllerEpoch) {
        val stateControllerEpochErrorMessage = ("Broker %d received update metadata request with correlation id %d from an " +
          "old controller %d with epoch %d. Latest known controller epoch is %d").format(localBrokerId,
          correlationId, updateMetadataRequest.controllerId, updateMetadataRequest.controllerEpoch,
          controllerEpoch)
        stateChangeLogger.warn(stateControllerEpochErrorMessage)
        throw new ControllerMovedException(stateControllerEpochErrorMessage)
      } else {
        metadataCache.updateCache(correlationId, updateMetadataRequest)
        controllerEpoch = updateMetadataRequest.controllerEpoch
      }
    }
  }

  def becomeLeaderOrFollower(correlationId: Int,leaderAndISRRequest: LeaderAndIsrRequest,
                             metadataCache: MetadataCache,
                             onLeadershipChange: (Iterable[Partition], Iterable[Partition]) => Unit): BecomeLeaderOrFollowerResult = {
    leaderAndISRRequest.partitionStates.asScala.foreach { case (topicPartition, stateInfo) =>
      stateChangeLogger.trace("Broker %d received LeaderAndIsr request %s correlation id %d from controller %d epoch %d for partition [%s,%d]"
                                .format(localBrokerId, stateInfo, correlationId,
                                        leaderAndISRRequest.controllerId, leaderAndISRRequest.controllerEpoch, topicPartition.topic, topicPartition.partition))
    }
    replicaStateChangeLock synchronized {
      val responseMap = new mutable.HashMap[TopicPartition, Short]
      if (leaderAndISRRequest.controllerEpoch < controllerEpoch) {
        leaderAndISRRequest.partitionStates.asScala.foreach { case (topicPartition, stateInfo) =>
        stateChangeLogger.warn(("Broker %d ignoring LeaderAndIsr request from controller %d with correlation id %d since " +
          "its controller epoch %d is old. Latest known controller epoch is %d").format(localBrokerId, leaderAndISRRequest.controllerId,
          correlationId, leaderAndISRRequest.controllerEpoch, controllerEpoch))
        }
        BecomeLeaderOrFollowerResult(responseMap, Errors.STALE_CONTROLLER_EPOCH.code)
      } else {
        val controllerId = leaderAndISRRequest.controllerId
        controllerEpoch = leaderAndISRRequest.controllerEpoch

        // First check partition's leader epoch
        val partitionState = new mutable.HashMap[Partition, PartitionState]()
        leaderAndISRRequest.partitionStates.asScala.foreach { case (topicPartition, stateInfo) =>
          val partition = getOrCreatePartition(topicPartition.topic, topicPartition.partition)
          val partitionLeaderEpoch = partition.getLeaderEpoch()
          // If the leader epoch is valid record the epoch of the controller that made the leadership decision.
          // This is useful while updating the isr to maintain the decision maker controller's epoch in the zookeeper path
          if (partitionLeaderEpoch < stateInfo.leaderEpoch) {
            if(stateInfo.replicas.contains(config.brokerId))
              partitionState.put(partition, stateInfo)
            else {
              stateChangeLogger.warn(("Broker %d ignoring LeaderAndIsr request from controller %d with correlation id %d " +
                "epoch %d for partition [%s,%d] as itself is not in assigned replica list %s")
                .format(localBrokerId, controllerId, correlationId, leaderAndISRRequest.controllerEpoch,
                  topicPartition.topic, topicPartition.partition, stateInfo.replicas.asScala.mkString(",")))
              responseMap.put(topicPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
            }
          } else {
            // Otherwise record the error code in response
            stateChangeLogger.warn(("Broker %d ignoring LeaderAndIsr request from controller %d with correlation id %d " +
              "epoch %d for partition [%s,%d] since its associated leader epoch %d is old. Current leader epoch is %d")
              .format(localBrokerId, controllerId, correlationId, leaderAndISRRequest.controllerEpoch,
                topicPartition.topic, topicPartition.partition, stateInfo.leaderEpoch, partitionLeaderEpoch))
            responseMap.put(topicPartition, Errors.STALE_CONTROLLER_EPOCH.code)
          }
        }

        val partitionsTobeLeader = partitionState.filter { case (partition, stateInfo) =>
          stateInfo.leader == config.brokerId
        }
        val partitionsToBeFollower = (partitionState -- partitionsTobeLeader.keys)

        val partitionsBecomeLeader = if (!partitionsTobeLeader.isEmpty)
          makeLeaders(controllerId, controllerEpoch, partitionsTobeLeader, correlationId, responseMap)
        else
          Set.empty[Partition]
        val partitionsBecomeFollower = if (!partitionsToBeFollower.isEmpty)
          makeFollowers(controllerId, controllerEpoch, partitionsToBeFollower, correlationId, responseMap, metadataCache)
        else
          Set.empty[Partition]

        // we initialize highwatermark thread after the first leaderisrrequest. This ensures that all the partitions
        // have been completely populated before starting the checkpointing there by avoiding weird race conditions
        if (!hwThreadInitialized) {
          startHighWaterMarksCheckPointThread()
          hwThreadInitialized = true
        }
        replicaFetcherManager.shutdownIdleFetcherThreads()

        onLeadershipChange(partitionsBecomeLeader, partitionsBecomeFollower)
        BecomeLeaderOrFollowerResult(responseMap, Errors.NONE.code)
      }
    }
  }

  /*
   * Make the current broker to become leader for a given set of partitions by:
   *
   * 1. Stop fetchers for these partitions
   * 2. Update the partition metadata in cache
   * 3. Add these partitions to the leader partitions set
   *
   * If an unexpected error is thrown in this function, it will be propagated to KafkaApis where
   * the error message will be set on each partition since we do not know which partition caused it. Otherwise,
   * return the set of partitions that are made leader due to this method
   *
   *  TODO: the above may need to be fixed later
   */
  private def makeLeaders(controllerId: Int,
                          epoch: Int,
                          partitionState: Map[Partition, PartitionState],
                          correlationId: Int,
                          responseMap: mutable.Map[TopicPartition, Short]): Set[Partition] = {
    partitionState.foreach(state =>
      stateChangeLogger.trace(("Broker %d handling LeaderAndIsr request correlationId %d from controller %d epoch %d " +
        "starting the become-leader transition for partition %s")
        .format(localBrokerId, correlationId, controllerId, epoch, TopicAndPartition(state._1.topic, state._1.partitionId))))

    for (partition <- partitionState.keys)
      responseMap.put(new TopicPartition(partition.topic, partition.partitionId), Errors.NONE.code)

    val partitionsToMakeLeaders: mutable.Set[Partition] = mutable.Set()

    try {
      // First stop fetchers for all the partitions
      replicaFetcherManager.removeFetcherForPartitions(partitionState.keySet.map(new TopicAndPartition(_)))
      // Update the partition information to be the leader
      partitionState.foreach{ case (partition, partitionStateInfo) =>
        if (partition.makeLeader(controllerId, partitionStateInfo, correlationId))
          partitionsToMakeLeaders += partition
        else
          stateChangeLogger.info(("Broker %d skipped the become-leader state change after marking its partition as leader with correlation id %d from " +
            "controller %d epoch %d for partition %s since it is already the leader for the partition.")
            .format(localBrokerId, correlationId, controllerId, epoch, TopicAndPartition(partition.topic, partition.partitionId)));
      }
      partitionsToMakeLeaders.foreach { partition =>
        stateChangeLogger.trace(("Broker %d stopped fetchers as part of become-leader request from controller " +
          "%d epoch %d with correlation id %d for partition %s")
          .format(localBrokerId, controllerId, epoch, correlationId, TopicAndPartition(partition.topic, partition.partitionId)))
      }
    } catch {
      case e: Throwable =>
        partitionState.foreach { state =>
          val errorMsg = ("Error on broker %d while processing LeaderAndIsr request correlationId %d received from controller %d" +
            " epoch %d for partition %s").format(localBrokerId, correlationId, controllerId, epoch,
                                                TopicAndPartition(state._1.topic, state._1.partitionId))
          stateChangeLogger.error(errorMsg, e)
        }
        // Re-throw the exception for it to be caught in KafkaApis
        throw e
    }

    partitionState.foreach { state =>
      stateChangeLogger.trace(("Broker %d completed LeaderAndIsr request correlationId %d from controller %d epoch %d " +
        "for the become-leader transition for partition %s")
        .format(localBrokerId, correlationId, controllerId, epoch, TopicAndPartition(state._1.topic, state._1.partitionId)))
    }

    partitionsToMakeLeaders
  }

  /*
   *
   * Make the current broker to become follower for a given set of partitions by:
   *
   * 1. Remove these partitions from the leader partitions set.
   *
   * 2. Mark the replicas as followers so that no more data can be added from the producer clients.
   *
   * 3. Stop fetchers for these partitions so that no more data can be added by the replica fetcher threads.
   *
   * 4. Truncate the log and checkpoint offsets for these partitions.
   *
   * 5. Clear the produce and fetch requests in the purgatory
   *
   * 6. If the broker is not shutting down, add the fetcher to the new leaders.
   *
   * The ordering of doing these steps make sure that the replicas in transition will not
   * take any more messages before checkpointing offsets so that all messages before the checkpoint
   * are guaranteed to be flushed to disks
   *
   * If an unexpected error is thrown in this function, it will be propagated to KafkaApis where
   * the error message will be set on each partition since we do not know which partition caused it. Otherwise,
   * return the set of partitions that are made follower due to this method
   */
  private def makeFollowers(controllerId: Int,
                            epoch: Int,
                            partitionState: Map[Partition, PartitionState],
                            correlationId: Int,
                            responseMap: mutable.Map[TopicPartition, Short],
                            metadataCache: MetadataCache) : Set[Partition] = {
    partitionState.foreach { state =>
      stateChangeLogger.trace(("Broker %d handling LeaderAndIsr request correlationId %d from controller %d epoch %d " +
        "starting the become-follower transition for partition %s")
        .format(localBrokerId, correlationId, controllerId, epoch, TopicAndPartition(state._1.topic, state._1.partitionId)))
    }

    for (partition <- partitionState.keys)
      responseMap.put(new TopicPartition(partition.topic, partition.partitionId), Errors.NONE.code)

    val partitionsToMakeFollower: mutable.Set[Partition] = mutable.Set()

    try {

      // TODO: Delete leaders from LeaderAndIsrRequest
      partitionState.foreach{ case (partition, partitionStateInfo) =>
        val newLeaderBrokerId = partitionStateInfo.leader
        metadataCache.getAliveBrokers.find(_.id == newLeaderBrokerId) match {
          // Only change partition state when the leader is available
          case Some(leaderBroker) =>
            if (partition.makeFollower(controllerId, partitionStateInfo, correlationId))
              partitionsToMakeFollower += partition
            else
              stateChangeLogger.info(("Broker %d skipped the become-follower state change after marking its partition as follower with correlation id %d from " +
                "controller %d epoch %d for partition [%s,%d] since the new leader %d is the same as the old leader")
                .format(localBrokerId, correlationId, controllerId, partitionStateInfo.controllerEpoch,
                partition.topic, partition.partitionId, newLeaderBrokerId))
          case None =>
            // The leader broker should always be present in the metadata cache.
            // If not, we should record the error message and abort the transition process for this partition
            stateChangeLogger.error(("Broker %d received LeaderAndIsrRequest with correlation id %d from controller" +
              " %d epoch %d for partition [%s,%d] but cannot become follower since the new leader %d is unavailable.")
              .format(localBrokerId, correlationId, controllerId, partitionStateInfo.controllerEpoch,
              partition.topic, partition.partitionId, newLeaderBrokerId))
            // Create the local replica even if the leader is unavailable. This is required to ensure that we include
            // the partition's high watermark in the checkpoint file (see KAFKA-1647)
            partition.getOrCreateReplica()
        }
      }

      replicaFetcherManager.removeFetcherForPartitions(partitionsToMakeFollower.map(new TopicAndPartition(_)))
      partitionsToMakeFollower.foreach { partition =>
        stateChangeLogger.trace(("Broker %d stopped fetchers as part of become-follower request from controller " +
          "%d epoch %d with correlation id %d for partition %s")
          .format(localBrokerId, controllerId, epoch, correlationId, TopicAndPartition(partition.topic, partition.partitionId)))
      }

      logManager.truncateTo(partitionsToMakeFollower.map(partition => (new TopicAndPartition(partition), partition.getOrCreateReplica().highWatermark.messageOffset)).toMap)
      partitionsToMakeFollower.foreach { partition =>
        val topicPartitionOperationKey = new TopicPartitionOperationKey(partition.topic, partition.partitionId)
        tryCompleteDelayedProduce(topicPartitionOperationKey)
        tryCompleteDelayedFetch(topicPartitionOperationKey)
      }

      partitionsToMakeFollower.foreach { partition =>
        stateChangeLogger.trace(("Broker %d truncated logs and checkpointed recovery boundaries for partition [%s,%d] as part of " +
          "become-follower request with correlation id %d from controller %d epoch %d").format(localBrokerId,
          partition.topic, partition.partitionId, correlationId, controllerId, epoch))
      }

      if (isShuttingDown.get()) {
        partitionsToMakeFollower.foreach { partition =>
          stateChangeLogger.trace(("Broker %d skipped the adding-fetcher step of the become-follower state change with correlation id %d from " +
            "controller %d epoch %d for partition [%s,%d] since it is shutting down").format(localBrokerId, correlationId,
            controllerId, epoch, partition.topic, partition.partitionId))
        }
      }
      else {
        // we do not need to check if the leader exists again since this has been done at the beginning of this process
        val partitionsToMakeFollowerWithLeaderAndOffset = partitionsToMakeFollower.map(partition =>
          new TopicAndPartition(partition) -> BrokerAndInitialOffset(
            metadataCache.getAliveBrokers.find(_.id == partition.leaderReplicaIdOpt.get).get.getBrokerEndPoint(config.interBrokerSecurityProtocol),
            partition.getReplica().get.logEndOffset.messageOffset)).toMap
        replicaFetcherManager.addFetcherForPartitions(partitionsToMakeFollowerWithLeaderAndOffset)

        partitionsToMakeFollower.foreach { partition =>
          stateChangeLogger.trace(("Broker %d started fetcher to new leader as part of become-follower request from controller " +
            "%d epoch %d with correlation id %d for partition [%s,%d]")
            .format(localBrokerId, controllerId, epoch, correlationId, partition.topic, partition.partitionId))
        }
      }
    } catch {
      case e: Throwable =>
        val errorMsg = ("Error on broker %d while processing LeaderAndIsr request with correlationId %d received from controller %d " +
          "epoch %d").format(localBrokerId, correlationId, controllerId, epoch)
        stateChangeLogger.error(errorMsg, e)
        // Re-throw the exception for it to be caught in KafkaApis
        throw e
    }

    partitionState.foreach { state =>
      stateChangeLogger.trace(("Broker %d completed LeaderAndIsr request correlationId %d from controller %d epoch %d " +
        "for the become-follower transition for partition %s")
        .format(localBrokerId, correlationId, controllerId, epoch, TopicAndPartition(state._1.topic, state._1.partitionId)))
    }

    partitionsToMakeFollower
  }

  // 评估分区的ISR列表，查看哪些副本可以从ISR列表中删除
  private def maybeShrinkIsr(): Unit = {
    trace("Evaluating ISR list of partitions to see which replicas can be removed from the ISR")
    // 遍历所有的topic-partiton的副本
    allPartitions.values.foreach(partition => partition.maybeShrinkIsr(config.replicaLagTimeMaxMs))
  }

  /**
    * 更新本地follower副本的LEO和推动HW
    * @param replicaId 副本ID
    * @param readResults 要返回给副本的日志列表
    */
  private def updateFollowerLogReadResults(replicaId: Int, readResults: Map[TopicAndPartition, LogReadResult]) {
    debug("Recording follower broker %d log read results: %s ".format(replicaId, readResults))
    readResults.foreach { case (topicAndPartition, readResult) =>
      getPartition(topicAndPartition.topic, topicAndPartition.partition) match {
        case Some(partition) =>
          // 更新本地follower副本的leo
          partition.updateReplicaLogReadResult(replicaId, readResult)

          // for producer requests with ack > 1, we need to check
          // if they can be unblocked after some follower's log end offsets have moved

          tryCompleteDelayedProduce(new TopicPartitionOperationKey(topicAndPartition))
        case None =>
          warn("While recording the replica LEO, the partition %s hasn't been created.".format(topicAndPartition))
      }
    }
  }

  // 过滤出leader副本在当前broker上的所有分区
  private def getLeaderPartitions() : List[Partition] = {
    allPartitions.values.filter(_.leaderReplicaIfLocal().isDefined).toList
  }

  // Flushes the highwatermark value for all partitions to the highwatermark file
  // 将各个分区的hw写入replication-offset-checkpoint文件
  def checkpointHighWatermarks() {
    // 获取所有分区
    val replicas = allPartitions.values.flatMap(_.getReplica(config.brokerId))
    // 过滤出log对象不为空的副本，然后获取对应的路径
    val replicasByDir = replicas.filter(_.log.isDefined).groupBy(_.log.get.dir.getParentFile.getAbsolutePath)
    // 遍历
    for ((dir, reps) <- replicasByDir) {
      // hw
      val hwms = reps.map(r => new TopicAndPartition(r) -> r.highWatermark.messageOffset).toMap
      try {
        // 写入文件
        highWatermarkCheckpoints(dir).write(hwms)
      } catch {
        case e: IOException =>
          fatal("Error writing to highwatermark file: ", e)
          Runtime.getRuntime().halt(1)
      }
    }
  }

  // High watermark do not need to be checkpointed only when under unit tests
  def shutdown(checkpointHW: Boolean = true) {
    info("Shutting down")
    replicaFetcherManager.shutdown()
    delayedFetchPurgatory.shutdown()
    delayedProducePurgatory.shutdown()
    if (checkpointHW)
      checkpointHighWatermarks()
    info("Shut down completely")
  }
}
