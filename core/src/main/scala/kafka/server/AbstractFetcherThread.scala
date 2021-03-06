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

import java.util.concurrent.locks.ReentrantLock

import kafka.cluster.BrokerEndPoint
import kafka.consumer.PartitionTopicInfo
import kafka.message.{MessageAndOffset, ByteBufferMessageSet}
import kafka.utils.{Pool, ShutdownableThread, DelayedItem}
import kafka.common.{KafkaException, ClientIdAndBroker, TopicAndPartition}
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.CoreUtils.inLock
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.protocol.Errors
import AbstractFetcherThread._
import scala.collection.{mutable, Set, Map}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import com.yammer.metrics.core.Gauge

/**
 *  Abstract class for fetching data from multiple partitions from the same broker.
 */
abstract class AbstractFetcherThread(name: String,
                                     clientId: String,
                                     sourceBroker: BrokerEndPoint,
                                     fetchBackOffMs: Int = 0,
                                     isInterruptible: Boolean = true)
  extends ShutdownableThread(name, isInterruptible) {

  type REQ <: FetchRequest
  type PD <: PartitionData
  // 记录topic-partition和对应的PartitionFetchState(里面记录了下次拉取消息的offset)
  private val partitionMap = new mutable.HashMap[TopicAndPartition, PartitionFetchState] // a (topic, partition) -> partitionFetchState map
  // 锁
  private val partitionMapLock = new ReentrantLock
  // 锁等待队列
  private val partitionMapCond = partitionMapLock.newCondition()

  private val metricId = new ClientIdAndBroker(clientId, sourceBroker.host, sourceBroker.port)
  val fetcherStats = new FetcherStats(metricId)
  val fetcherLagStats = new FetcherLagStats(metricId)

  /* callbacks to be defined in subclass */

  // process fetched data
  /**
    * 处理Fetch到的数据
    * @param topicAndPartition
    * @param fetchOffset Fetch请求的起始offset
    * @param partitionData Fetch到的数据
    */
  def processPartitionData(topicAndPartition: TopicAndPartition, fetchOffset: Long, partitionData: PD)

  // handle a partition whose offset is out of range and return a new fetch offset
  /**
    * 处理Fetch偏移量超出范围的分区并返回一个新的读取偏移量
    * @param topicAndPartition
    * @return
    */
  def handleOffsetOutOfRange(topicAndPartition: TopicAndPartition): Long

  // deal with partitions with errors, potentially due to leadership changes
  /**
    * 处理Fetch出现错误的topic-partition
    * @param partitions
    */
  def handlePartitionsWithErrors(partitions: Iterable[TopicAndPartition])

  /**
    * 构建Fetch请求
    * @param partitionMap
    * @return
    */
  protected def buildFetchRequest(partitionMap: Map[TopicAndPartition, PartitionFetchState]): REQ

  /**
    * 发送Fetch请求
    * @param fetchRequest
    * @return
    */
  protected def fetch(fetchRequest: REQ): Map[TopicAndPartition, PD]

  override def shutdown(){
    initiateShutdown()
    inLock(partitionMapLock) {
      partitionMapCond.signalAll()
    }
    awaitShutdown()

    // we don't need the lock since the thread has finished shutdown and metric removal is safe
    fetcherStats.unregister()
    fetcherLagStats.unregister()
  }

  /**
    * 启动入口
    */
  override def doWork() {
    // 构建fetchRequest
    val fetchRequest = inLock(partitionMapLock) {
      // 构建fetch请求
      val fetchRequest = buildFetchRequest(partitionMap)
      if (fetchRequest.isEmpty) {
        trace("There are no active partitions. Back off for %d ms before sending a fetch request".format(fetchBackOffMs))
        // 阻塞等待replica.fetch.backoff.ms，默认1000
        partitionMapCond.await(fetchBackOffMs, TimeUnit.MILLISECONDS)
      }
      // 返回fetchRequest
      fetchRequest
    }
    // 如果fetchRequest不为空，发送
    if (!fetchRequest.isEmpty)
      // 处理fetchRequest
      processFetchRequest(fetchRequest)
  }

  /**
    * 处理发送fetch请求
    * @param fetchRequest
    */
  private def processFetchRequest(fetchRequest: REQ) {
    // 用于记录拉取异常的topic-partition
    val partitionsWithError = new mutable.HashSet[TopicAndPartition]
    var responseData: Map[TopicAndPartition, PD] = Map.empty
    /*-------------步骤一 构建fetch请求并等待响应-------------*/
    try {
      trace("Issuing to broker %d of fetch request %s".format(sourceBroker.id, fetchRequest))
      // 发送并等待响应
      responseData = fetch(fetchRequest)
    } catch {
      case t: Throwable =>
        if (isRunning.get) {
          warn(s"Error in fetch $fetchRequest", t)
          inLock(partitionMapLock) {
            partitionsWithError ++= partitionMap.keys
            // there is an error occurred while fetching partitions, sleep a while
            partitionMapCond.await(fetchBackOffMs, TimeUnit.MILLISECONDS)
          }
        }
    }
    fetcherStats.requestRate.mark()
    /*-------------步骤二 处理响应-------------*/
    if (responseData.nonEmpty) {
      // process fetched data
      inLock(partitionMapLock) {
        // 遍历响应获取每个topic-partition的响应数据PartitionData
        responseData.foreach { case (topicAndPartition, partitionData) =>
          val TopicAndPartition(topic, partitionId) = topicAndPartition
          // 从partitionMap中获取对应topic-partition的PartitionFetchState
          partitionMap.get(topicAndPartition).foreach(currentPartitionFetchState =>
            // we append to the log if the current offset is defined and it is the same as the offset requested during fetch
            // 如果对应topic-partition的fetchRequest中的起始offset和PartitionFetchState中记录的offset一致，那么才继续处理响应
            if (fetchRequest.offset(topicAndPartition) == currentPartitionFetchState.offset) {
              Errors.forCode(partitionData.errorCode) match {
                // 处理无异常的响应数据
                case Errors.NONE =>
                  try {
                    // 从响应数据PartitionData中获取消息日志
                    val messages: ByteBufferMessageSet = partitionData.toByteBufferMessageSet
                    val validBytes: Int = messages.validBytes
                    // 获取消息集合中最后一条消息的offset + 1(用于生产下次fetch的offset)
                    val newOffset = messages.shallowIterator.toSeq.lastOption match {
                      case Some(m: MessageAndOffset) => m.nextOffset
                      case None => currentPartitionFetchState.offset
                    }
                    // 更新下次fetch的起始offset
                    partitionMap.put(topicAndPartition, new PartitionFetchState(newOffset))
                    fetcherLagStats.getAndMaybePut(topic, partitionId).lag = Math.max(0L, partitionData.highWatermark - newOffset)
                    fetcherStats.byteRate.mark(validBytes)
                    // Once we hand off the partition data to the subclass, we can't mess with it any more in this thread
                    // 处理数据
                    processPartitionData(topicAndPartition, currentPartitionFetchState.offset, partitionData)
                  } catch {
                    case ime: CorruptRecordException =>
                      // we log the error and continue. This ensures two things
                      // 1. If there is a corrupt message in a topic partition, it does not bring the fetcher thread down and cause other topic partition to also lag
                      // 2. If the message is corrupt due to a transient state in the log (truncation, partial writes can cause this), we simply continue and
                      // should get fixed in the subsequent fetches
                      logger.error("Found invalid messages during fetch for partition [" + topic + "," + partitionId + "] offset " + currentPartitionFetchState.offset  + " error " + ime.getMessage)
                    case e: Throwable =>
                      throw new KafkaException("error processing data for partition [%s,%d] offset %d"
                        .format(topic, partitionId, currentPartitionFetchState.offset), e)
                  }
                // 处理Errors.OFFSET_OUT_OF_RANGE 异常的响应数据
                case Errors.OFFSET_OUT_OF_RANGE =>
                  try {
                    // 计算fetch的起始offset，这里分为两种情况
                    val newOffset = handleOffsetOutOfRange(topicAndPartition)
                    // 创建新的PartitionFetchState，替换旧的，也就是更新下次fetch的起始offset(follower的LEO)
                    partitionMap.put(topicAndPartition, new PartitionFetchState(newOffset))
                    error("Current offset %d for partition [%s,%d] out of range; reset offset to %d"
                      .format(currentPartitionFetchState.offset, topic, partitionId, newOffset))
                  } catch {
                    case e: Throwable =>
                      error("Error getting offset for partition [%s,%d] to broker %d".format(topic, partitionId, sourceBroker.id), e)
                      partitionsWithError += topicAndPartition
                  }
                // 处理其他异常
                case _ =>
                  if (isRunning.get) {
                    error("Error for partition [%s,%d] to broker %d:%s".format(topic, partitionId, sourceBroker.id,
                      partitionData.exception.get))
                    partitionsWithError += topicAndPartition
                  }
              }
            })
        }
      }
    }
    /*-------------步骤二 处理拉取消息出现异常的topic-partition-------------*/
    if (partitionsWithError.nonEmpty) {
      debug("handling partitions with error for %s".format(partitionsWithError))
      handlePartitionsWithErrors(partitionsWithError)
    }
  }

  /**
    * 添加对某些topic-partition的fetch操作
    * 调用点{@link kafka.server.AbstractFetcherManager#addFetcherForPartitions(scala.collection.Map)}
    * @param partitionAndOffsets
    */
  def addPartitions(partitionAndOffsets: Map[TopicAndPartition, Long]) {
    partitionMapLock.lockInterruptibly()
    try {
      // 记录从各个topic-partition拉取消息的位置到partitionMap中，如果offset < 0说明当前Broker之前不是该topic-partition的副本
      // 现在才是，由于没有日志offset为-1，所以调用handleOffsetOutOfRange()方法从Leader副本拉取消息来判断需要从哪个offset拉取消息
      for ((topicAndPartition, offset) <- partitionAndOffsets) {
        // If the partitionMap already has the topic/partition, then do not update the map with the old offset
        if (!partitionMap.contains(topicAndPartition))
          partitionMap.put(
            topicAndPartition,
            if (PartitionTopicInfo.isOffsetInvalid(offset)) new PartitionFetchState(handleOffsetOutOfRange(topicAndPartition))
            else new PartitionFetchState(offset)
          )}
      partitionMapCond.signalAll()
    } finally partitionMapLock.unlock()
  }

  // 将topic-partition的PartitionFetchState更新为延迟的
  def delayPartitions(partitions: Iterable[TopicAndPartition], delay: Long) {
    partitionMapLock.lockInterruptibly()
    try {
      for (partition <- partitions) {
        partitionMap.get(partition).foreach (currentPartitionFetchState =>
          if (currentPartitionFetchState.isActive)
            // 更新
            partitionMap.put(partition, new PartitionFetchState(currentPartitionFetchState.offset, new DelayedItem(delay)))
        )
      }
      partitionMapCond.signalAll()
    } finally partitionMapLock.unlock()
  }

  /**
    * 删除对某些topic-partition的fetch操作
    * 调用点{@link kafka.server.AbstractFetcherManager#removeFetcherForPartitions(scala.collection.Set)}
    * @param topicAndPartitions
    */
  def removePartitions(topicAndPartitions: Set[TopicAndPartition]) {
    partitionMapLock.lockInterruptibly()
    try {
      topicAndPartitions.foreach { topicAndPartition =>
        partitionMap.remove(topicAndPartition)
        fetcherLagStats.unregister(topicAndPartition.topic, topicAndPartition.partition)
      }
    } finally partitionMapLock.unlock()
  }

  def partitionCount() = {
    partitionMapLock.lockInterruptibly()
    try partitionMap.size
    finally partitionMapLock.unlock()
  }

}

object AbstractFetcherThread {

  trait FetchRequest {
    def isEmpty: Boolean
    def offset(topicAndPartition: TopicAndPartition): Long
  }

  trait PartitionData {
    def errorCode: Short
    def exception: Option[Throwable]
    def toByteBufferMessageSet: ByteBufferMessageSet
    def highWatermark: Long
  }

}

object FetcherMetrics {
  val ConsumerLag = "ConsumerLag"
  val RequestsPerSec = "RequestsPerSec"
  val BytesPerSec = "BytesPerSec"
}

class FetcherLagMetrics(metricId: ClientIdTopicPartition) extends KafkaMetricsGroup {

  private[this] val lagVal = new AtomicLong(-1L)
  private[this] val tags = Map(
    "clientId" -> metricId.clientId,
    "topic" -> metricId.topic,
    "partition" -> metricId.partitionId.toString)

  newGauge(FetcherMetrics.ConsumerLag,
    new Gauge[Long] {
      def value = lagVal.get
    },
    tags
  )

  def lag_=(newLag: Long) {
    lagVal.set(newLag)
  }

  def lag = lagVal.get

  def unregister() {
    removeMetric(FetcherMetrics.ConsumerLag, tags)
  }
}

class FetcherLagStats(metricId: ClientIdAndBroker) {
  private val valueFactory = (k: ClientIdTopicPartition) => new FetcherLagMetrics(k)
  val stats = new Pool[ClientIdTopicPartition, FetcherLagMetrics](Some(valueFactory))

  def getAndMaybePut(topic: String, partitionId: Int): FetcherLagMetrics = {
    stats.getAndMaybePut(new ClientIdTopicPartition(metricId.clientId, topic, partitionId))
  }

  def unregister(topic: String, partitionId: Int) {
    val lagMetrics = stats.remove(new ClientIdTopicPartition(metricId.clientId, topic, partitionId))
    if (lagMetrics != null) lagMetrics.unregister()
  }

  def unregister() {
    stats.keys.toBuffer.foreach { key: ClientIdTopicPartition =>
      unregister(key.topic, key.partitionId)
    }
  }
}

class FetcherStats(metricId: ClientIdAndBroker) extends KafkaMetricsGroup {
  val tags = Map("clientId" -> metricId.clientId,
    "brokerHost" -> metricId.brokerHost,
    "brokerPort" -> metricId.brokerPort.toString)

  val requestRate = newMeter(FetcherMetrics.RequestsPerSec, "requests", TimeUnit.SECONDS, tags)

  val byteRate = newMeter(FetcherMetrics.BytesPerSec, "bytes", TimeUnit.SECONDS, tags)

  def unregister() {
    removeMetric(FetcherMetrics.RequestsPerSec, tags)
    removeMetric(FetcherMetrics.BytesPerSec, tags)
  }

}

case class ClientIdTopicPartition(clientId: String, topic: String, partitionId: Int) {
  override def toString = "%s-%s-%d".format(clientId, topic, partitionId)
}

/**
  * case class to keep partition offset and its state(active , inactive)
  * 保留了分区的偏移量和对应的状态
  */
case class PartitionFetchState(offset: Long, delay: DelayedItem) {
  // 默认为alive
  def this(offset: Long) = this(offset, new DelayedItem(0))
  // 判断是否为alive，只有到期的才会变为alive
  def isActive: Boolean = { delay.getDelay(TimeUnit.MILLISECONDS) == 0 }

  override def toString = "%d-%b".format(offset, isActive)
}
