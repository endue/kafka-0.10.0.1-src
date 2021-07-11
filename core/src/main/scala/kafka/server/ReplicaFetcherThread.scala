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

import java.net.SocketTimeoutException

import kafka.admin.AdminUtils
import kafka.cluster.{BrokerEndPoint, Replica}
import kafka.log.LogConfig
import kafka.message.ByteBufferMessageSet
import kafka.api.{KAFKA_0_10_0_IV0, KAFKA_0_9_0}
import kafka.common.{KafkaStorageException, TopicAndPartition}
import ReplicaFetcherThread._
import org.apache.kafka.clients.{ClientRequest, ClientResponse, ManualMetadataUpdater, NetworkClient}
import org.apache.kafka.common.network.{ChannelBuilders, LoginType, Mode, NetworkReceive, Selectable, Selector}
import org.apache.kafka.common.requests.{AbstractRequest, FetchResponse, ListOffsetRequest, ListOffsetResponse, RequestSend}
import org.apache.kafka.common.requests.{FetchRequest => JFetchRequest}
import org.apache.kafka.common.security.ssl.SslFactory
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.utils.Time

import scala.collection.{JavaConverters, Map, mutable}
import JavaConverters._

class ReplicaFetcherThread(name: String,
                           fetcherId: Int,
                           sourceBroker: BrokerEndPoint,
                           brokerConfig: KafkaConfig,
                           replicaMgr: ReplicaManager,
                           metrics: Metrics,
                           time: Time)
  extends AbstractFetcherThread(name = name,
                                clientId = name,
                                sourceBroker = sourceBroker,
                                fetchBackOffMs = brokerConfig.replicaFetchBackoffMs,
                                isInterruptible = false) {
  // 声明两个别名
  type REQ = FetchRequest
  type PD = PartitionData
  // fetch请求的版本
  private val fetchRequestVersion: Short =
    if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_10_0_IV0) 2
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_9_0) 1
    else 0
  /**
    * socket请求超时时间,默认30 * 1000
    * replica.socket.timeout.ms
    */
  private val socketTimeout: Int = brokerConfig.replicaSocketTimeoutMs
  // 当前broker的ID
  private val replicaId = brokerConfig.brokerId
  /**
    * 每次拉取最大等待时间，默认500ms
    * replica.fetch.wait.max.ms
    */
  private val maxWait = brokerConfig.replicaFetchWaitMaxMs
  /**
    * 每次拉取最少的字节数,默认1
    * replica.fetch.min.bytes
    */
  private val minBytes = brokerConfig.replicaFetchMinBytes
  /**
    * 每次fetch最大字节数，默认1M
    * replica.fetch.max.bytes
    */
  private val fetchSize = brokerConfig.replicaFetchMaxBytes
  // 线程名
  private def clientId = name
  // 记录要拉取的leader副本的相关信息
  private val sourceNode = new Node(sourceBroker.id, sourceBroker.host, sourceBroker.port)

  // we need to include both the broker id and the fetcher id
  // as the metrics tag to avoid metric name conflicts with
  // more than one fetcher thread to the same broker
  // 创建一个网络连接
  private val networkClient = {
    val channelBuilder = ChannelBuilders.create(
      brokerConfig.interBrokerSecurityProtocol,
      Mode.CLIENT,
      LoginType.SERVER,
      brokerConfig.values,
      brokerConfig.saslMechanismInterBrokerProtocol,
      brokerConfig.saslInterBrokerHandshakeRequestEnable
    )
    val selector = new Selector(
      NetworkReceive.UNLIMITED,
      brokerConfig.connectionsMaxIdleMs,
      metrics,
      time,
      "replica-fetcher",
      Map("broker-id" -> sourceBroker.id.toString, "fetcher-id" -> fetcherId.toString).asJava,
      false,
      channelBuilder
    )
    new NetworkClient(
      selector,
      new ManualMetadataUpdater(),
      clientId,
      1,
      0,
      Selectable.USE_DEFAULT_BUFFER_SIZE,
      brokerConfig.replicaSocketReceiveBufferBytes,
      brokerConfig.requestTimeoutMs,
      time
    )
  }

  // 关闭fetcher 线程，起始就是关闭底层的忘了连接
  override def shutdown(): Unit = {
    super.shutdown()
    networkClient.close()
  }

  // process fetched data
  /**
    * 处理fetch到的数据
    * @param topicAndPartition
    * @param fetchOffset Fetch请求的起始offset
    * @param partitionData Fetch到的数据
    */
  def processPartitionData(topicAndPartition: TopicAndPartition, fetchOffset: Long, partitionData: PartitionData) {
    try {
      val TopicAndPartition(topic, partitionId) = topicAndPartition
      // 获取对应topic-partition的副本
      val replica = replicaMgr.getReplica(topic, partitionId).get
      // 获取响应数据
      val messageSet = partitionData.toByteBufferMessageSet
      warnIfMessageOversized(messageSet, topicAndPartition)

      if (fetchOffset != replica.logEndOffset.messageOffset)
        throw new RuntimeException("Offset mismatch for partition %s: fetched offset = %d, log end offset = %d.".format(topicAndPartition, fetchOffset, replica.logEndOffset.messageOffset))
      if (logger.isTraceEnabled)
        trace("Follower %d has replica log end offset %d for partition %s. Received %d messages and leader hw %d"
          .format(replica.brokerId, replica.logEndOffset.messageOffset, topicAndPartition, messageSet.sizeInBytes, partitionData.highWatermark))
      // 处理消息到本地Log
      replica.log.get.append(messageSet, assignOffsets = false)
      if (logger.isTraceEnabled)
        trace("Follower %d has replica log end offset %d after appending %d bytes of messages for partition %s"
          .format(replica.brokerId, replica.logEndOffset.messageOffset, messageSet.sizeInBytes, topicAndPartition))
      // 在follower的LEO和leader的HW中取其最小者，作为follower的新的HW
      val followerHighWatermark = replica.logEndOffset.messageOffset.min(partitionData.highWatermark)
      // for the follower replica, we do not need to keep
      // its segment base offset the physical position,
      // these values will be computed upon making the leader
      // 更新follower的HW
      replica.highWatermark = new LogOffsetMetadata(followerHighWatermark)
      if (logger.isTraceEnabled)
        trace("Follower %d set replica high watermark for partition [%s,%d] to %s"
          .format(replica.brokerId, topic, partitionId, followerHighWatermark))
    } catch {
      case e: KafkaStorageException =>
        fatal(s"Disk error while replicating data for $topicAndPartition", e)
        Runtime.getRuntime.halt(1)
    }
  }

  def warnIfMessageOversized(messageSet: ByteBufferMessageSet, topicAndPartition: TopicAndPartition): Unit = {
    if (messageSet.sizeInBytes > 0 && messageSet.validBytes <= 0)
      error(s"Replication is failing due to a message that is greater than replica.fetch.max.bytes for partition $topicAndPartition. " +
        "This generally occurs when the max.message.bytes has been overridden to exceed this value and a suitably large " +
        "message has also been sent. To fix this problem increase replica.fetch.max.bytes in your broker config to be " +
        "equal or larger than your settings for max.message.bytes, both at a broker and topic level.")
  }

  /**
   * Handle a partition whose offset is out of range and return a new fetch offset.
    * 处理fetch请求的offset超过了leader存储的偏移量，那么重新计算一个偏移量返回
   */
  def handleOffsetOutOfRange(topicAndPartition: TopicAndPartition): Long = {
    // 获取对应topic-partition的副本
    val replica: Replica = replicaMgr.getReplica(topicAndPartition.topic, topicAndPartition.partition).get

    /**
     * Unclean leader election: A follower goes down, in the meanwhile the leader keeps appending messages. The follower comes back up
     * and before it has completely caught up with the leader's logs, all replicas in the ISR go down. The follower is now uncleanly
     * elected as the new leader, and it starts appending messages from the client. The old leader comes back up, becomes a follower
     * and it may discover that the current leader's end offset is behind its own end offset.
     *
     * In such a case, truncate the current follower's log to the current leader's end offset and continue fetching.
     *
     * There is a potential for a mismatch between the logs of the two replicas here. We don't fix this mismatch as of now.
     * 由于某种原因导致ISR列表外的副本被选为了Leader，之后旧Leader的LEO会出现大于新Leader的LEO的情况
     */
    // 获取Leader的LEO
    val leaderEndOffset: Long = earliestOrLatestOffset(topicAndPartition, ListOffsetRequest.LATEST_TIMESTAMP,
      brokerConfig.brokerId)
    // Leader的LEO小于当前follower副本的LEO
    if (leaderEndOffset < replica.logEndOffset.messageOffset) {
      // Prior to truncating the follower's log, ensure that doing so is not disallowed by the configuration for unclean leader election.
      // This situation could only happen if the unclean election configuration for a topic changes while a replica is down. Otherwise,
      // we should never encounter this situation since a non-ISR leader cannot be elected if disallowed by the broker configuration.
      // unclean.leader.election.enable参数值为false，就意味着非ISR中的副本不能够参与选举,关闭当前的broker
      if (!LogConfig.fromProps(brokerConfig.originals, AdminUtils.fetchEntityConfig(replicaMgr.zkUtils,
        ConfigType.Topic, topicAndPartition.topic)).uncleanLeaderElectionEnable) {
        // Log a fatal error and shutdown the broker to ensure that data loss does not unexpectedly occur.
        fatal("Exiting because log truncation is not allowed for topic %s,".format(topicAndPartition.topic) +
          " Current leader %d's latest offset %d is less than replica %d's latest offset %d"
          .format(sourceBroker.id, leaderEndOffset, brokerConfig.brokerId, replica.logEndOffset.messageOffset))
        System.exit(1)
      }

      warn("Replica %d for partition %s reset its fetch offset from %d to current leader %d's latest offset %d"
        .format(brokerConfig.brokerId, topicAndPartition, replica.logEndOffset.messageOffset, sourceBroker.id, leaderEndOffset))
      // unclean.leader.election.enable参数值为true,也就是非ISR中的副本可以参与选举，那么此时截断当前副本上的日志消息到Leader的LEO的位置
      replicaMgr.logManager.truncateTo(Map(topicAndPartition -> leaderEndOffset))
      // 返回Leader的LEO
      leaderEndOffset
    // Leader的LEO大于等于当前follower副本的LEO
    } else {
      /**
       * If the leader's log end offset is greater than the follower's log end offset, there are two possibilities:
        * 如果leader的LEO大于follower的LEO，那么有两种可能
       * 1. The follower could have been down for a long time and when it starts up, its end offset could be smaller than the leader's
       * start offset because the leader has deleted old logs (log.logEndOffset < leaderStartOffset).
       * 当前follower关闭一段时间在启动后，它的LEO会小于Leader的start offset，因为可能此时Leader删除了旧的日志
       * 2. When unclean leader election occurs, it is possible that the old leader's high watermark is greater than
       * the new leader's log end offset. So when the old leader truncates its offset to its high watermark and starts
       * to fetch from the new leader, an OffsetOutOfRangeException will be thrown. After that some more messages are
       * produced to the new leader. While the old leader is trying to handle the OffsetOutOfRangeException and query
       * the log end offset of the new leader, the new leader's log end offset becomes higher than the follower's log end offset.
       * 当unclean.leader.election.enable参数值为true时，旧Leader的HW可能会高于新Leader的LEO，所有旧Leader从HW位置开始拉取消息时会出现
       * OffsetOutOfRangeException，之后客户端产生消息添加到新Leader，旧Leader处理OffsetOutOfRangeException然后查询新Leader的LEO，此时
       * 新Leader的LEO会大于旧Leader的LEO
       * In the first case, the follower's current log end offset is smaller than the leader's log start offset. So the
       * follower should truncate all its logs, roll out a new segment and start to fetch from the current leader's log
       * start offset.
       * 在第一个情况中，follower截断自己的日志，并从Leader的start offset开始创建一个新的日志段来拉取存储消息
       * In the second case, the follower should just keep the current log segments and retry the fetch. In the second
       * case, there will be some inconsistency of data between old and new leader. We are not solving it here.
       * If users want to have strong consistency guarantees, appropriate configurations needs to be set for both
       * brokers and producers.
       * 在第二个情况中，会出现数据不一致的问题，当前还未解决
       * Putting the two cases together, the follower should fetch from the higher one of its replica log end offset
       * and the current leader's log start offset.
       *
       */
        //获取Leader的start offset
      val leaderStartOffset: Long = earliestOrLatestOffset(topicAndPartition, ListOffsetRequest.EARLIEST_TIMESTAMP,
        brokerConfig.brokerId)
      warn("Replica %d for partition %s reset its fetch offset from %d to current leader %d's start offset %d"
        .format(brokerConfig.brokerId, topicAndPartition, replica.logEndOffset.messageOffset, sourceBroker.id, leaderStartOffset))
      // 在follower的LEO和Leader的start offset中计算出最大值，作为offsetToFetch
      val offsetToFetch = Math.max(leaderStartOffset, replica.logEndOffset.messageOffset)
      // Only truncate log when current leader's log start offset is greater than follower's log end offset.
      // Leader的start offset大于当前follower的LEO，删除follower该topic-partition的所有日志，然后基于start offset创建LogSegment
      // 这种可能就是Leader的旧日志被删除了，follower的旧日志也要删除
      if (leaderStartOffset > replica.logEndOffset.messageOffset)
        replicaMgr.logManager.truncateFullyAndStartAt(topicAndPartition, leaderStartOffset)
      // 返回offsetToFetch(Leader的start offset或者follower的LEO)
      offsetToFetch
    }
  }

  // any logic for partitions whose leader has changed
  /**
    * 处理fetch请求出现问题的topic-partition
    * @param partitions
    */
  def handlePartitionsWithErrors(partitions: Iterable[TopicAndPartition]) {
    // replica.fetch.backoff.ms,默认1000
    delayPartitions(partitions, brokerConfig.replicaFetchBackoffMs.toLong)
  }

  /**
    * 发送fetch请求
    * @param fetchRequest
    * @return
    */
  protected def fetch(fetchRequest: FetchRequest): Map[TopicAndPartition, PartitionData] =  {
    // 发送fetch请求并获取响应
    val clientResponse = sendRequest(ApiKeys.FETCH, Some(fetchRequestVersion), fetchRequest.underlying)
    // 将响应按照topic-partition进行分组，之后返回FetchResponse
    new FetchResponse(clientResponse.responseBody).responseData.asScala.map { case (key, value) =>
      TopicAndPartition(key.topic, key.partition) -> new PartitionData(value)
    }
  }

  /**
    * 发送请求
    * @param apiKey 请求类型
    * @param apiVersion 请求版本
    * @param request 请求
    * @return
    */
  private def sendRequest(apiKey: ApiKeys, apiVersion: Option[Short], request: AbstractRequest): ClientResponse = {
    import kafka.utils.NetworkClientBlockingOps._
    // 构建请求头
    val header = apiVersion.fold(networkClient.nextRequestHeader(apiKey))(networkClient.nextRequestHeader(apiKey, _))
    try {
      if (!networkClient.blockingReady(sourceNode, socketTimeout)(time))
        throw new SocketTimeoutException(s"Failed to connect within $socketTimeout ms")
      else {
        // 构建reuqest
        val send = new RequestSend(sourceBroker.id.toString, header, request.toStruct)
        val clientRequest = new ClientRequest(time.milliseconds(), true, send, null)
        // 发送reuqest并阻塞等待响应，收到响应后返回ClientResponse
        networkClient.blockingSendAndReceive(clientRequest)(time)
      }
    }
    catch {
      case e: Throwable =>
        networkClient.close(sourceBroker.id.toString)
        throw e
    }

  }

  // 获取leader副本上一定范围内所有LogSegment中最大的那个的baseOffset
  private def earliestOrLatestOffset(topicAndPartition: TopicAndPartition, earliestOrLatest: Long, consumerId: Int): Long = {
    val topicPartition = new TopicPartition(topicAndPartition.topic, topicAndPartition.partition)
    val partitions = Map(
      topicPartition -> new ListOffsetRequest.PartitionData(earliestOrLatest, 1)
    )
    // 创建一个LIST_OFFSETS请求
    val request = new ListOffsetRequest(consumerId, partitions.asJava)
    // 发送LIST_OFFSETS请求并等待响应
    val clientResponse = sendRequest(ApiKeys.LIST_OFFSETS, None, request)
    val response = new ListOffsetResponse(clientResponse.responseBody)
    // 获取leader上当前topic-partition的一定范围内所有LogSegment的baseOffset
    // 是从大到下排列的
    val partitionData: ListOffsetResponse.PartitionData = response.responseData.get(topicPartition)
    Errors.forCode(partitionData.errorCode) match {
        // 获取最大的LogSegment的baseOffset
      case Errors.NONE => partitionData.offsets.asScala.head
      case errorCode => throw errorCode.exception
    }
  }

  /**
    * 构建fetch request
    * @param partitionMap 记录需要拉取消息的topic-partition以及起始offset
    * @return
    */
  protected def buildFetchRequest(partitionMap: Map[TopicAndPartition, PartitionFetchState]): FetchRequest = {
    // 记录topic-partition对应的PartitionData
    val requestMap = mutable.Map.empty[TopicPartition, JFetchRequest.PartitionData]
    // 构建拉取每个topic-partition的requestMap
    partitionMap.foreach { case ((TopicAndPartition(topic, partition), partitionFetchState)) =>
      // 过滤出活跃的PartitionFetchState
      if (partitionFetchState.isActive)
        // 拉取的时候从PartitionFetchState中获取从哪个offset开始拉取最多拉取1M
        requestMap(new TopicPartition(topic, partition)) = new JFetchRequest.PartitionData(partitionFetchState.offset, fetchSize)
    }
    // 构建FetchRequest，最少拉取1个字节，如果没有则等待maxWait = 500ms
    new FetchRequest(new JFetchRequest(replicaId, maxWait, minBytes, requestMap.asJava))
  }

}

object ReplicaFetcherThread {

  private[server] class FetchRequest(val underlying: JFetchRequest) extends AbstractFetcherThread.FetchRequest {
    def isEmpty: Boolean = underlying.fetchData.isEmpty
    def offset(topicAndPartition: TopicAndPartition): Long =
      underlying.fetchData.asScala(new TopicPartition(topicAndPartition.topic, topicAndPartition.partition)).offset
  }

  private[server] class PartitionData(val underlying: FetchResponse.PartitionData) extends AbstractFetcherThread.PartitionData {
    // 异常码
    def errorCode: Short = underlying.errorCode
    // 消息集
    def toByteBufferMessageSet: ByteBufferMessageSet = new ByteBufferMessageSet(underlying.recordSet)
    // leader副本的HE
    def highWatermark: Long = underlying.highWatermark
    // 异常类型
    def exception: Option[Throwable] = Errors.forCode(errorCode) match {
      case Errors.NONE => None
      case e => Some(e.exception)
    }

  }

}
