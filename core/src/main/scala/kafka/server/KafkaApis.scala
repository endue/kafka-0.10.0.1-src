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

import java.nio.ByteBuffer
import java.lang.{Long => JLong, Short => JShort}
import java.util.Properties

import kafka.admin.{AdminUtils, RackAwareMode}
import kafka.api._
import kafka.cluster.Partition
import kafka.common
import kafka.common._
import kafka.controller.KafkaController
import kafka.coordinator.{GroupCoordinator, JoinGroupResult}
import kafka.log._
import kafka.message.{ByteBufferMessageSet, Message, MessageSet}
import kafka.network._
import kafka.network.RequestChannel.{Response, Session}
import kafka.security.auth.{Authorizer, ClusterAction, Create, Describe, Group, Operation, Read, Resource, Topic, Write}
import kafka.utils.{Logging, SystemTime, ZKGroupTopicDirs, ZkUtils}
import org.apache.kafka.common.errors.{ClusterAuthorizationException, InvalidTopicException, NotLeaderForPartitionException, UnknownTopicOrPartitionException}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.{ApiKeys, Errors, Protocol, SecurityProtocol}
import org.apache.kafka.common.requests.{ApiVersionsResponse, DescribeGroupsRequest, DescribeGroupsResponse, GroupCoordinatorRequest, GroupCoordinatorResponse, HeartbeatRequest, HeartbeatResponse, JoinGroupRequest, JoinGroupResponse, LeaderAndIsrRequest, LeaderAndIsrResponse, LeaveGroupRequest, LeaveGroupResponse, ListGroupsResponse, ListOffsetRequest, ListOffsetResponse, MetadataRequest, MetadataResponse, OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse, ProduceRequest, ProduceResponse, ResponseHeader, ResponseSend, StopReplicaRequest, StopReplicaResponse, SyncGroupRequest, SyncGroupResponse, UpdateMetadataRequest, UpdateMetadataResponse}
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.common.internals.TopicConstants

import scala.collection._
import scala.collection.JavaConverters._
import org.apache.kafka.common.requests.SaslHandshakeResponse

/**
 * Logic to handle the various Kafka requests
  * 处理各种各样的逻辑请求
 */
class KafkaApis(val requestChannel: RequestChannel,
                val replicaManager: ReplicaManager,
                val coordinator: GroupCoordinator,
                val controller: KafkaController,
                val zkUtils: ZkUtils,
                val brokerId: Int,
                val config: KafkaConfig,
                val metadataCache: MetadataCache,
                val metrics: Metrics,
                val authorizer: Option[Authorizer]) extends Logging {

  this.logIdent = "[KafkaApi-%d] ".format(brokerId)
  // Store all the quota managers for each type of request
  val quotaManagers: Map[Short, ClientQuotaManager] = instantiateQuotaManagers(config)

  /**
   * Top-level method that handles all requests and multiplexes to the right api
   */
  def handle(request: RequestChannel.Request) {
    try {
      trace("Handling request:%s from connection %s;securityProtocol:%s,principal:%s".
        format(request.requestDesc(true), request.connectionId, request.securityProtocol, request.session.principal))
      ApiKeys.forId(request.requestId) match {
        case ApiKeys.PRODUCE => handleProducerRequest(request)
        case ApiKeys.FETCH => handleFetchRequest(request)
        case ApiKeys.LIST_OFFSETS => handleOffsetRequest(request)
        case ApiKeys.METADATA => handleTopicMetadataRequest(request)
        case ApiKeys.LEADER_AND_ISR => handleLeaderAndIsrRequest(request)
        case ApiKeys.STOP_REPLICA => handleStopReplicaRequest(request)
        case ApiKeys.UPDATE_METADATA_KEY => handleUpdateMetadataRequest(request)
        case ApiKeys.CONTROLLED_SHUTDOWN_KEY => handleControlledShutdownRequest(request)
        case ApiKeys.OFFSET_COMMIT => handleOffsetCommitRequest(request)
        case ApiKeys.OFFSET_FETCH => handleOffsetFetchRequest(request)
        case ApiKeys.GROUP_COORDINATOR => handleGroupCoordinatorRequest(request)
        case ApiKeys.JOIN_GROUP => handleJoinGroupRequest(request)
        case ApiKeys.HEARTBEAT => handleHeartbeatRequest(request)
        case ApiKeys.LEAVE_GROUP => handleLeaveGroupRequest(request)
        case ApiKeys.SYNC_GROUP => handleSyncGroupRequest(request)
        case ApiKeys.DESCRIBE_GROUPS => handleDescribeGroupRequest(request)
        case ApiKeys.LIST_GROUPS => handleListGroupsRequest(request)
        case ApiKeys.SASL_HANDSHAKE => handleSaslHandshakeRequest(request)
        case ApiKeys.API_VERSIONS => handleApiVersionsRequest(request)
        case requestId => throw new KafkaException("Unknown api code " + requestId)
      }
    } catch {
      case e: Throwable =>
        if (request.requestObj != null) {
          request.requestObj.handleError(e, requestChannel, request)
          error("Error when handling request %s".format(request.requestObj), e)
        } else {
          val response = request.body.getErrorResponse(request.header.apiVersion, e)
          val respHeader = new ResponseHeader(request.header.correlationId)

          /* If request doesn't have a default error response, we just close the connection.
             For example, when produce request has acks set to 0 */
          if (response == null)
            requestChannel.closeConnection(request.processor, request)
          else
            requestChannel.sendResponse(new Response(request, new ResponseSend(request.connectionId, respHeader, response)))

          error("Error when handling request %s".format(request.body), e)
        }
    } finally
      request.apiLocalCompleteTimeMs = SystemTime.milliseconds
  }

  // 处理LEADER_AND_ISR请求
  def handleLeaderAndIsrRequest(request: RequestChannel.Request) {
    // ensureTopicExists is only for client facing requests
    // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they
    // stop serving data to clients for the topic being deleted
    val correlationId: Int = request.header.correlationId
    val leaderAndIsrRequest: LeaderAndIsrRequest = request.body.asInstanceOf[LeaderAndIsrRequest]

    try {
      // 回调方法
      // 之前是follower现在变成了leader、之前是leader现在变成了follower
      def onLeadershipChange(updatedLeaders: Iterable[Partition], updatedFollowers: Iterable[Partition]) {
        // for each new leader or follower, call coordinator to handle consumer group migration.
        // this callback is invoked under the replica state change lock to ensure proper order of
        // leadership changes
        // 当前broker成为内部队列__consumer_offset的某些Partition的leader
        // 遍历这些个Partition，恢复commit offset等内容，这些需要leader来维护
        updatedLeaders.foreach { partition =>
          // 校验是否为内部队列
          if (partition.topic == TopicConstants.GROUP_METADATA_TOPIC_NAME)
            coordinator.handleGroupImmigration(partition.partitionId)
        }
        // 当前broker成为内部队列__consumer_offset的某些Partition的follower
        // 遍历这些个Partition，删除缓存的一些数据，已经是follower了，这些应该是leader来维护
        updatedFollowers.foreach { partition =>
          if (partition.topic == TopicConstants.GROUP_METADATA_TOPIC_NAME)
            coordinator.handleGroupEmigration(partition.partitionId)
        }
      }

      val responseHeader = new ResponseHeader(correlationId)
      val leaderAndIsrResponse =
        if (authorize(request.session, ClusterAction, Resource.ClusterResource)) {
          val result = replicaManager.becomeLeaderOrFollower(correlationId, leaderAndIsrRequest, metadataCache, onLeadershipChange)
          new LeaderAndIsrResponse(result.errorCode, result.responseMap.mapValues(new JShort(_)).asJava)
        } else {
          val result = leaderAndIsrRequest.partitionStates.asScala.keys.map((_, new JShort(Errors.CLUSTER_AUTHORIZATION_FAILED.code))).toMap
          new LeaderAndIsrResponse(Errors.CLUSTER_AUTHORIZATION_FAILED.code, result.asJava)
        }

      requestChannel.sendResponse(new Response(request, new ResponseSend(request.connectionId, responseHeader, leaderAndIsrResponse)))
    } catch {
      case e: KafkaStorageException =>
        fatal("Disk error during leadership change.", e)
        Runtime.getRuntime.halt(1)
    }
  }


  /**
    * 处理STOP_REPLICA请求
    * @param request
    */
  def handleStopReplicaRequest(request: RequestChannel.Request) {
    // ensureTopicExists is only for client facing requests
    // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they
    // stop serving data to clients for the topic being deleted
    val stopReplicaRequest = request.body.asInstanceOf[StopReplicaRequest]

    val responseHeader = new ResponseHeader(request.header.correlationId)
    val response: StopReplicaResponse =
      if (authorize(request.session, ClusterAction, Resource.ClusterResource)) {
        val (result, error) = replicaManager.stopReplicas(stopReplicaRequest)
        new StopReplicaResponse(error, result.asInstanceOf[Map[TopicPartition, JShort]].asJava)
      } else {
        val result = stopReplicaRequest.partitions.asScala.map((_, new JShort(Errors.CLUSTER_AUTHORIZATION_FAILED.code))).toMap
        new StopReplicaResponse(Errors.CLUSTER_AUTHORIZATION_FAILED.code, result.asJava)
      }

    requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, responseHeader, response)))
    replicaManager.replicaFetcherManager.shutdownIdleFetcherThreads()
  }
  // 处理UPDATE_METADATA_KEY请求
  def handleUpdateMetadataRequest(request: RequestChannel.Request) {
    val correlationId = request.header.correlationId
    val updateMetadataRequest = request.body.asInstanceOf[UpdateMetadataRequest]

    val updateMetadataResponse =
      if (authorize(request.session, ClusterAction, Resource.ClusterResource)) {
        replicaManager.maybeUpdateMetadataCache(correlationId, updateMetadataRequest, metadataCache)
        new UpdateMetadataResponse(Errors.NONE.code)
      } else {
        new UpdateMetadataResponse(Errors.CLUSTER_AUTHORIZATION_FAILED.code)
      }

    val responseHeader = new ResponseHeader(correlationId)
    requestChannel.sendResponse(new Response(request, new ResponseSend(request.connectionId, responseHeader, updateMetadataResponse)))
  }

  def handleControlledShutdownRequest(request: RequestChannel.Request) {
    // ensureTopicExists is only for client facing requests
    // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they
    // stop serving data to clients for the topic being deleted
    val controlledShutdownRequest = request.requestObj.asInstanceOf[ControlledShutdownRequest]

    authorizeClusterAction(request)

    val partitionsRemaining = controller.shutdownBroker(controlledShutdownRequest.brokerId)
    val controlledShutdownResponse = new ControlledShutdownResponse(controlledShutdownRequest.correlationId,
      Errors.NONE.code, partitionsRemaining)
    requestChannel.sendResponse(new Response(request, new RequestOrResponseSend(request.connectionId, controlledShutdownResponse)))
  }


  /**
   * Handle an offset commit request
   */
  def handleOffsetCommitRequest(request: RequestChannel.Request) {
    val header = request.header
    val offsetCommitRequest = request.body.asInstanceOf[OffsetCommitRequest]

    // reject the request if not authorized to the group
    if (!authorize(request.session, Read, new Resource(Group, offsetCommitRequest.groupId))) {
      val errorCode = new JShort(Errors.GROUP_AUTHORIZATION_FAILED.code)
      val results = offsetCommitRequest.offsetData.keySet.asScala.map { topicPartition =>
        (topicPartition, errorCode)
      }.toMap
      val responseHeader = new ResponseHeader(header.correlationId)
      val responseBody = new OffsetCommitResponse(results.asJava)
      requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, responseHeader, responseBody)))
    } else {
      // filter non-existent topics
      val invalidRequestsInfo = offsetCommitRequest.offsetData.asScala.filter { case (topicPartition, _) =>
        !metadataCache.contains(topicPartition.topic)
      }
      val filteredRequestInfo = offsetCommitRequest.offsetData.asScala.toMap -- invalidRequestsInfo.keys

      val (authorizedRequestInfo, unauthorizedRequestInfo) = filteredRequestInfo.partition {
        case (topicPartition, offsetMetadata) => authorize(request.session, Read, new Resource(Topic, topicPartition.topic))
      }

      // the callback for sending an offset commit response
      def sendResponseCallback(commitStatus: immutable.Map[TopicPartition, Short]) {
        val mergedCommitStatus = commitStatus ++ unauthorizedRequestInfo.mapValues(_ => Errors.TOPIC_AUTHORIZATION_FAILED.code)

        mergedCommitStatus.foreach { case (topicPartition, errorCode) =>
          if (errorCode != Errors.NONE.code) {
            debug(s"Offset commit request with correlation id ${header.correlationId} from client ${header.clientId} " +
              s"on partition $topicPartition failed due to ${Errors.forCode(errorCode).exceptionName}")
          }
        }
        val combinedCommitStatus = mergedCommitStatus.mapValues(new JShort(_)) ++ invalidRequestsInfo.map(_._1 -> new JShort(Errors.UNKNOWN_TOPIC_OR_PARTITION.code))

        val responseHeader = new ResponseHeader(header.correlationId)
        val responseBody = new OffsetCommitResponse(combinedCommitStatus.asJava)
        requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, responseHeader, responseBody)))
      }

      if (authorizedRequestInfo.isEmpty)
        sendResponseCallback(Map.empty)
      // 如果是版本0，存储偏移量到zk
      else if (header.apiVersion == 0) {
        // for version 0 always store offsets to ZK
        val responseInfo = authorizedRequestInfo.map {
          case (topicPartition, partitionData) =>
            val topicDirs = new ZKGroupTopicDirs(offsetCommitRequest.groupId, topicPartition.topic)
            try {
              if (!metadataCache.hasTopicMetadata(topicPartition.topic))
                (topicPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
              else if (partitionData.metadata != null && partitionData.metadata.length > config.offsetMetadataMaxSize)
                (topicPartition, Errors.OFFSET_METADATA_TOO_LARGE.code)
              else {
                // 更新zk上的offset  /consumers/{group}/ids/offsets/{topic}/{partition}
                zkUtils.updatePersistentPath(s"${topicDirs.consumerOffsetDir}/${topicPartition.partition}", partitionData.offset.toString)
                (topicPartition, Errors.NONE.code)
              }
            } catch {
              case e: Throwable => (topicPartition, Errors.forException(e).code)
            }
        }
        sendResponseCallback(responseInfo)
      // 如果是版本1，存储偏移量到offset manager
      } else {
        // for version 1 and beyond store offsets in offset manager

        // compute the retention time based on the request version:
        // 根据请求版本计算保留时间
        // if it is v1 or not specified by user, we can use the default retention
        // 如果是v1或者用户没有指定，我们可以使用默认保留
        val offsetRetention =
          if (header.apiVersion <= 1 ||
            offsetCommitRequest.retentionTime == OffsetCommitRequest.DEFAULT_RETENTION_TIME)
            coordinator.offsetConfig.offsetsRetentionMs
          else
            offsetCommitRequest.retentionTime

        // commit timestamp is always set to now.
        // "default" expiration timestamp is now + retention (and retention may be overridden if v2)
        // expire timestamp is computed differently for v1 and v2.
        //   - If v1 and no explicit commit timestamp is provided we use default expiration timestamp.
        //   - If v1 and explicit commit timestamp is provided we calculate retention from that explicit commit timestamp
        //   - If v2 we use the default expiration timestamp
        val currentTimestamp = SystemTime.milliseconds
        val defaultExpireTimestamp = offsetRetention + currentTimestamp
        val partitionData = authorizedRequestInfo.mapValues { partitionData =>
          val metadata = if (partitionData.metadata == null) OffsetMetadata.NoMetadata else partitionData.metadata;
          new OffsetAndMetadata(
            offsetMetadata = OffsetMetadata(partitionData.offset, metadata),
            commitTimestamp = currentTimestamp,
            expireTimestamp = {
              if (partitionData.timestamp == OffsetCommitRequest.DEFAULT_TIMESTAMP)
                defaultExpireTimestamp
              else
                offsetRetention + partitionData.timestamp
            }
          )
        }

        // call coordinator to handle commit offset
        coordinator.handleCommitOffsets(
          offsetCommitRequest.groupId,
          offsetCommitRequest.memberId,
          offsetCommitRequest.generationId,
          partitionData,
          sendResponseCallback)
      }
    }
  }

  private def authorize(session: Session, operation: Operation, resource: Resource): Boolean =
    authorizer.map(_.authorize(session, operation, resource)).getOrElse(true)

  /**
   * Handle a produce request
    * 处理Producer请求
   */
  def handleProducerRequest(request: RequestChannel.Request) {
    val produceRequest = request.body.asInstanceOf[ProduceRequest]
    val numBytesAppended = request.header.sizeOf + produceRequest.sizeOf

    val (authorizedRequestInfo, unauthorizedRequestInfo): (mutable.Map[TopicPartition, ByteBuffer], mutable.Map[TopicPartition, ByteBuffer]) = produceRequest.partitionRecords.asScala.partition {
      case (topicPartition, _) => authorize(request.session, Write, new Resource(Topic, topicPartition.topic))
    }

    // the callback for sending a produce response
    // 处理完消息的回调
    def sendResponseCallback(responseStatus: Map[TopicPartition, PartitionResponse]) {

      val mergedResponseStatus = responseStatus ++ unauthorizedRequestInfo.mapValues(_ =>
        new PartitionResponse(Errors.TOPIC_AUTHORIZATION_FAILED.code, -1, Message.NoTimestamp))

      var errorInResponse = false

      mergedResponseStatus.foreach { case (topicPartition, status) =>
        if (status.errorCode != Errors.NONE.code) {
          errorInResponse = true
          debug("Produce request with correlation id %d from client %s on partition %s failed due to %s".format(
            request.header.correlationId,
            request.header.clientId,
            topicPartition,
            Errors.forCode(status.errorCode).exceptionName))
        }
      }

      def produceResponseCallback(delayTimeMs: Int) {
        // 判断消息生产端acks机制
        if (produceRequest.acks == 0) {
          // no operation needed if producer request.required.acks = 0; however, if there is any error in handling
          // the request, since no response is expected by the producer, the server will close socket server so that
          // the producer client will know that some error has happened and will refresh its metadata
          if (errorInResponse) {
            val exceptionsSummary = mergedResponseStatus.map { case (topicPartition, status) =>
              topicPartition -> Errors.forCode(status.errorCode).exceptionName
            }.mkString(", ")
            info(
              s"Closing connection due to error during produce request with correlation id ${request.header.correlationId} " +
                s"from client id ${request.header.clientId} with ack=0\n" +
                s"Topic and partition to exceptions: $exceptionsSummary"
            )
            requestChannel.closeConnection(request.processor, request)
          } else {
            requestChannel.noOperation(request.processor, request)
          }
        } else {
          val respHeader = new ResponseHeader(request.header.correlationId)
          val respBody = request.header.apiVersion match {
            case 0 => new ProduceResponse(mergedResponseStatus.asJava)
            case version@(1 | 2) => new ProduceResponse(mergedResponseStatus.asJava, delayTimeMs, version)
            // This case shouldn't happen unless a new version of ProducerRequest is added without
            // updating this part of the code to handle it properly.
            case version => throw new IllegalArgumentException(s"Version `$version` of ProduceRequest is not handled. Code must be updated.")
          }
          // 发送响应
          requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, respHeader, respBody)))
        }
      }

      // When this callback is triggered, the remote API call has completed
      request.apiRemoteCompleteTimeMs = SystemTime.milliseconds

      quotaManagers(ApiKeys.PRODUCE.id).recordAndMaybeThrottle(
        request.header.clientId,
        numBytesAppended,
        produceResponseCallback)
    }

    if (authorizedRequestInfo.isEmpty)
      sendResponseCallback(Map.empty)
    else {
      // 是否将消息添加到kafka内部队列，只有请求的客户端是"__admin_client"才可以
      val internalTopicsAllowed = request.header.clientId == AdminUtils.AdminClientId

      // Convert ByteBuffer to ByteBufferMessageSet
      // 转换各个topic-partition的消息集合ByteBuffer为ByteBufferMessageSet
      val authorizedMessagesPerPartition: mutable.Map[TopicPartition, ByteBufferMessageSet]  = authorizedRequestInfo.map {
        case (topicPartition, buffer) => (topicPartition, new ByteBufferMessageSet(buffer))
      }

      // call the replica manager to append messages to the replicas
      // 调用replicaManager组件处理消息
      replicaManager.appendMessages(
        produceRequest.timeout.toLong,
        produceRequest.acks,
        internalTopicsAllowed,
        authorizedMessagesPerPartition,
        sendResponseCallback)

      // if the request is put into the purgatory, it will have a held reference
      // and hence cannot be garbage collected; hence we clear its data here in
      // order to let GC re-claim its memory since it is already appended to log
      produceRequest.clearPartitionRecords()
    }
  }

  /**
   * Handle a fetch request
    * leader处理fetch请求
   */
  def handleFetchRequest(request: RequestChannel.Request) {
    val fetchRequest: FetchRequest = request.requestObj.asInstanceOf[FetchRequest]

    val (authorizedRequestInfo, unauthorizedRequestInfo): (Predef.Map[TopicAndPartition, PartitionFetchInfo], Predef.Map[TopicAndPartition, PartitionFetchInfo]) = fetchRequest.requestInfo.partition {
      case (topicAndPartition, _) => authorize(request.session, Read, new Resource(Topic, topicAndPartition.topic))
    }

    val unauthorizedPartitionData = unauthorizedRequestInfo.mapValues { _ =>
      FetchResponsePartitionData(Errors.TOPIC_AUTHORIZATION_FAILED.code, -1, MessageSet.Empty)
    }

    // the callback for sending a fetch response
    // 回调函数
    def sendResponseCallback(responsePartitionData: Map[TopicAndPartition, FetchResponsePartitionData]) {

      val convertedPartitionData =
        // Need to down-convert message when consumer only takes magic value 0.
        if (fetchRequest.versionId <= 1) {
          responsePartitionData.map { case (tp, data) =>

            // We only do down-conversion when:
            // 1. The message format version configured for the topic is using magic value > 0, and
            // 2. The message set contains message whose magic > 0
            // This is to reduce the message format conversion as much as possible. The conversion will only occur
            // when new message format is used for the topic and we see an old request.
            // Please note that if the message format is changed from a higher version back to lower version this
            // test might break because some messages in new message format can be delivered to consumers before 0.10.0.0
            // without format down conversion.
            val convertedData = if (replicaManager.getMessageFormatVersion(tp).exists(_ > Message.MagicValue_V0) &&
              !data.messages.isMagicValueInAllWrapperMessages(Message.MagicValue_V0)) {
              trace(s"Down converting message to V0 for fetch request from ${fetchRequest.clientId}")
              new FetchResponsePartitionData(data.error, data.hw, data.messages.asInstanceOf[FileMessageSet].toMessageFormat(Message.MagicValue_V0))
            } else data

            tp -> convertedData
          }
        } else responsePartitionData

      val mergedPartitionData = convertedPartitionData ++ unauthorizedPartitionData

      mergedPartitionData.foreach { case (topicAndPartition, data) =>
        if (data.error != Errors.NONE.code)
          debug(s"Fetch request with correlation id ${fetchRequest.correlationId} from client ${fetchRequest.clientId} " +
            s"on partition $topicAndPartition failed due to ${Errors.forCode(data.error).exceptionName}")
        // record the bytes out metrics only when the response is being sent
        BrokerTopicStats.getBrokerTopicStats(topicAndPartition.topic).bytesOutRate.mark(data.messages.sizeInBytes)
        BrokerTopicStats.getBrokerAllTopicsStats().bytesOutRate.mark(data.messages.sizeInBytes)
      }

      def fetchResponseCallback(delayTimeMs: Int) {
        trace(s"Sending fetch response to client ${fetchRequest.clientId} of " +
          s"${convertedPartitionData.values.map(_.messages.sizeInBytes).sum} bytes")
        val response = FetchResponse(fetchRequest.correlationId, mergedPartitionData, fetchRequest.versionId, delayTimeMs)
        requestChannel.sendResponse(new RequestChannel.Response(request, new FetchResponseSend(request.connectionId, response)))
      }


      // When this callback is triggered, the remote API call has completed
      request.apiRemoteCompleteTimeMs = SystemTime.milliseconds

      // Do not throttle replication traffic
      if (fetchRequest.isFromFollower) {
        fetchResponseCallback(0)
      } else {
        quotaManagers(ApiKeys.FETCH.id).recordAndMaybeThrottle(fetchRequest.clientId,
                                                               FetchResponse.responseSize(mergedPartitionData.groupBy(_._1.topic),
                                                                                          fetchRequest.versionId),
                                                               fetchResponseCallback)
      }
    }

    if (authorizedRequestInfo.isEmpty)
      sendResponseCallback(Map.empty)
    else {
      // call the replica manager to fetch messages from the local replica
      // 拉取消息
      replicaManager.fetchMessages(
        fetchRequest.maxWait.toLong,
        fetchRequest.replicaId,
        fetchRequest.minBytes,
        authorizedRequestInfo,
        sendResponseCallback)
    }
  }

  /**
   * Handle an offset request
    *
    * 处理LIST_OFFSETS请求
    * 拉取偏移量请求
   */
  def handleOffsetRequest(request: RequestChannel.Request) {
    val correlationId = request.header.correlationId
    val clientId = request.header.clientId
    val offsetRequest = request.body.asInstanceOf[ListOffsetRequest]

    val (authorizedRequestInfo, unauthorizedRequestInfo) = offsetRequest.offsetData.asScala.partition {
      case (topicPartition, _) => authorize(request.session, Describe, new Resource(Topic, topicPartition.topic))
    }

    val unauthorizedResponseStatus = unauthorizedRequestInfo.mapValues(_ =>
      new ListOffsetResponse.PartitionData(Errors.TOPIC_AUTHORIZATION_FAILED.code, List[JLong]().asJava)
    )

    val responseMap = authorizedRequestInfo.map(elem => {
      val (topicPartition, partitionData) = elem
      try {
        // ensure leader exists
        // 获取本地对应此topic-partiton的副本
        val localReplica = if (offsetRequest.replicaId != ListOffsetRequest.DEBUGGING_REPLICA_ID)
          // 获取topic-partition的Partition实例，然后获取该实例中记录的leaderId，判断是否为当前Broker，如果是就返回对应的Replica
          // 如果不是那么抛出异常
          replicaManager.getLeaderReplicaIfLocal(topicPartition.topic, topicPartition.partition)
        else
          // 获取topic-partition的Partition实例，然后获取该实例中记录的当前brokerId对应的Relica
          // 如果没有那么就抛出异常
          replicaManager.getReplicaOrException(topicPartition.topic, topicPartition.partition)
        // 获取偏移量
        val offsets = {
          // 拉取偏移量
          // 如果是follower副本发生过来的请求，参考kafka.server.ReplicaFetcherThread.handleOffsetOutOfRange
          // partitionData.timestamp分为ListOffsetRequest.LATEST_TIMESTAMP和ListOffsetRequest.EARLIEST_TIMESTAMP
          // partitionData.maxNumOffsets为1
          val allOffsets = fetchOffsets(replicaManager.logManager,
                                        topicPartition,
                                        partitionData.timestamp,
                                        partitionData.maxNumOffsets)// kafkaConusmer设置为1
          // 从follower副本发送过来的请求
          if (offsetRequest.replicaId != ListOffsetRequest.CONSUMER_REPLICA_ID) {
            allOffsets
          // kafkaConsumer在发送请求时replicaId写死为-1，参考org.apache.kafka.clients.consumer.internals.Fetcher.sendListOffsetRequest
          } else {
            // 获取HW
            val hw = localReplica.highWatermark.messageOffset
            if (allOffsets.exists(_ > hw))
              hw +: allOffsets.dropWhile(_ > hw)
            else
              allOffsets
          }
        }
        // 返回响应
        (topicPartition, new ListOffsetResponse.PartitionData(Errors.NONE.code, offsets.map(new JLong(_)).asJava))
      } catch {
        // NOTE: UnknownTopicOrPartitionException and NotLeaderForPartitionException are special cased since these error messages
        // are typically transient and there is no value in logging the entire stack trace for the same
        case utpe: UnknownTopicOrPartitionException =>
          debug("Offset request with correlation id %d from client %s on partition %s failed due to %s".format(
               correlationId, clientId, topicPartition, utpe.getMessage))
          (topicPartition, new ListOffsetResponse.PartitionData(Errors.forException(utpe).code, List[JLong]().asJava))
        case nle: NotLeaderForPartitionException =>
          debug("Offset request with correlation id %d from client %s on partition %s failed due to %s".format(
               correlationId, clientId, topicPartition,nle.getMessage))
          (topicPartition, new ListOffsetResponse.PartitionData(Errors.forException(nle).code, List[JLong]().asJava))
        case e: Throwable =>
          error("Error while responding to offset request", e)
          (topicPartition, new ListOffsetResponse.PartitionData(Errors.forException(e).code, List[JLong]().asJava))
      }
    })

    val mergedResponseMap = responseMap ++ unauthorizedResponseStatus

    val responseHeader = new ResponseHeader(correlationId)
    val response = new ListOffsetResponse(mergedResponseMap.asJava)

    requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, responseHeader, response)))
  }

  /**
    * 拉取topic-partition的offsets
    * @param logManager
    * @param topicPartition
    * @param timestamp
    * @param maxNumOffsets
    * @return
    */
  def fetchOffsets(logManager: LogManager, topicPartition: TopicPartition, timestamp: Long, maxNumOffsets: Int): Seq[Long] = {
    // 查找对应topic-partition的Log对象(有了Log对象就有了该topic-partition写日志的文件)
    logManager.getLog(TopicAndPartition(topicPartition.topic, topicPartition.partition)) match {
      case Some(log) =>
        fetchOffsetsBefore(log, timestamp, maxNumOffsets)
      case None =>
        // 如果不存在，根据timestamp的类型返回不同值
        if (timestamp == ListOffsetRequest.LATEST_TIMESTAMP || timestamp == ListOffsetRequest.EARLIEST_TIMESTAMP)
          Seq(0L)
        else
          Nil
    }
  }

  private[server] def fetchOffsetsBefore(log: Log, timestamp: Long, maxNumOffsets: Int): Seq[Long] = {
    // 获取Log下所有LogSegment段
    val segsArray = log.logSegments.toArray
    // 初始化offsetTimeArray长度,用来记录LogSegment的baseOffset和lastModified
    var offsetTimeArray: Array[(Long, Long)] = null
    // 最新的LogSegment是否被写入了数据，
    val lastSegmentHasSize = segsArray.last.size > 0
    if (lastSegmentHasSize)
      offsetTimeArray = new Array[(Long, Long)](segsArray.length + 1)
    else
      offsetTimeArray = new Array[(Long, Long)](segsArray.length)
    // 遍历所有LogSegment的baseOffset和lastModified记录到offsetTimeArray数组中
    // 如果activeSegment有数据，那么取对应的LEO并且最后修改的时间为当前时间戳
    for (i <- 0 until segsArray.length)
      offsetTimeArray(i) = (segsArray(i).baseOffset, segsArray(i).lastModified)
    if (lastSegmentHasSize)
      offsetTimeArray(segsArray.length) = (log.logEndOffset, SystemTime.milliseconds)

    var startIndex = -1
    timestamp match {
      case ListOffsetRequest.LATEST_TIMESTAMP =>
        // 返回最后一个LogSegment的log.logEndOffset
        startIndex = offsetTimeArray.length - 1
      case ListOffsetRequest.EARLIEST_TIMESTAMP =>
        // 返回第一个LogSegment的baseOffset
        startIndex = 0
      // 如果没找到从后向前遍历，直到offsetTimeArray中存储的时间小于等于参数timestamp
      case _ =>
        var isFound = false
        debug("Offset time array = " + offsetTimeArray.foreach(o => "%d, %d".format(o._1, o._2)))
        startIndex = offsetTimeArray.length - 1
        while (startIndex >= 0 && !isFound) {
          if (offsetTimeArray(startIndex)._2 <= timestamp)
            isFound = true
          else
            startIndex -= 1
        }
    }
    // 计算返回几个offset
    val retSize = maxNumOffsets.min(startIndex + 1)
    val ret = new Array[Long](retSize)
    // 返回startIndex以及之前的所有Segment的baseOffset或者
    for (j <- 0 until retSize) {
      ret(j) = offsetTimeArray(startIndex)._1
      startIndex -= 1
    }
    // ensure that the returned seq is in descending order of offsets
    // 降序排列
    ret.toSeq.sortBy(-_)
  }

  /**
    * 创建topic
    * @param topic topic名称
    * @param numPartitions topic分区数
    * @param replicationFactor topic副本数
    * @param properties
    * @return
    */
  private def createTopic(topic: String,
                          numPartitions: Int,
                          replicationFactor: Int,
                          properties: Properties = new Properties()): MetadataResponse.TopicMetadata = {
    try {
      // 调用的方法和通过命令行创建时一致的
      AdminUtils.createTopic(zkUtils, topic, numPartitions, replicationFactor, properties, RackAwareMode.Safe)
      info("Auto creation of topic %s with %d partitions and replication factor %d is successful"
        .format(topic, numPartitions, replicationFactor))
      // 返回响应
      new MetadataResponse.TopicMetadata(Errors.LEADER_NOT_AVAILABLE, topic, common.Topic.isInternal(topic),
        java.util.Collections.emptyList())
    } catch {
      case e: TopicExistsException => // let it go, possibly another broker created this topic
        new MetadataResponse.TopicMetadata(Errors.LEADER_NOT_AVAILABLE, topic, common.Topic.isInternal(topic),
          java.util.Collections.emptyList())
      case itex: InvalidTopicException =>
        new MetadataResponse.TopicMetadata(Errors.INVALID_TOPIC_EXCEPTION, topic, common.Topic.isInternal(topic),
          java.util.Collections.emptyList())
    }
  }

  // 创建内部的__consumer_offsets topic
  private def createGroupMetadataTopic(): MetadataResponse.TopicMetadata = {
    val aliveBrokers = metadataCache.getAliveBrokers
    val offsetsTopicReplicationFactor =
      if (aliveBrokers.nonEmpty)
        Math.min(config.offsetsTopicReplicationFactor.toInt, aliveBrokers.length)
      else
        config.offsetsTopicReplicationFactor.toInt
    // 创建topic
    // GROUP_METADATA_TOPIC_NAME为__consumer_offsets topic
    // offsetsTopicPartitions为offsets.topic.num.partitions默认50
    createTopic(TopicConstants.GROUP_METADATA_TOPIC_NAME, config.offsetsTopicPartitions,
      offsetsTopicReplicationFactor, coordinator.offsetsTopicConfigs)
  }
  // 获取内部“__consumer_offsets”topic的元数据
  private def getOrCreateGroupMetadataTopic(securityProtocol: SecurityProtocol): MetadataResponse.TopicMetadata = {
    // 获取topic的元数据
    val topicMetadata = metadataCache.getTopicMetadata(Set(TopicConstants.GROUP_METADATA_TOPIC_NAME), securityProtocol)
    // 获取元数据，如果没有说明当前“__consumer_offsets”topic还未创建
    // 那么创建并获取对应的元数据
    topicMetadata.headOption.getOrElse(createGroupMetadataTopic())
  }

  // 获取topic的metadata
  private def getTopicMetadata(topics: Set[String], securityProtocol: SecurityProtocol, errorUnavailableEndpoints: Boolean): Seq[MetadataResponse.TopicMetadata] = {
    // 从metadataCache中获取
    val topicResponses = metadataCache.getTopicMetadata(topics, securityProtocol, errorUnavailableEndpoints)
    if (topics.isEmpty || topicResponses.size == topics.size) {
      topicResponses
    } else {
      // 获取缓存中不存在的topipc
      val nonExistentTopics = topics -- topicResponses.map(_.topic).toSet
      val responsesForNonExistentTopics = nonExistentTopics.map { topic =>
        if (topic == TopicConstants.GROUP_METADATA_TOPIC_NAME) {
          createGroupMetadataTopic()
        } else if (config.autoCreateTopicsEnable) {// auto.create.topics.enable，默认true
          // 创建topic
          createTopic(topic, config.numPartitions, config.defaultReplicationFactor)
        } else {
          new MetadataResponse.TopicMetadata(Errors.UNKNOWN_TOPIC_OR_PARTITION, topic, common.Topic.isInternal(topic),
            java.util.Collections.emptyList())
        }
      }
      topicResponses ++ responsesForNonExistentTopics
    }
  }

  /**
   * Handle a topic metadata request
   */
  def handleTopicMetadataRequest(request: RequestChannel.Request) {
    val metadataRequest = request.body.asInstanceOf[MetadataRequest]
    val requestVersion = request.header.apiVersion()

    val topics =
      // Handle old metadata request logic. Version 0 has no way to specify "no topics".
      if (requestVersion == 0) {
        if (metadataRequest.topics() == null || metadataRequest.topics().isEmpty)
          metadataCache.getAllTopics()
        else
          metadataRequest.topics.asScala.toSet
      } else {
        if (metadataRequest.isAllTopics)
          metadataCache.getAllTopics()
        else
          metadataRequest.topics.asScala.toSet
      }

    var (authorizedTopics, unauthorizedTopics) =
      topics.partition(topic => authorize(request.session, Describe, new Resource(Topic, topic)))

    if (authorizedTopics.nonEmpty) {
      val nonExistingTopics = metadataCache.getNonExistingTopics(authorizedTopics)
      if (config.autoCreateTopicsEnable && nonExistingTopics.nonEmpty) {
        authorizer.foreach { az =>
          if (!az.authorize(request.session, Create, Resource.ClusterResource)) {
            authorizedTopics --= nonExistingTopics
            unauthorizedTopics ++= nonExistingTopics
          }
        }
      }
    }

    val unauthorizedTopicMetadata = unauthorizedTopics.map(topic =>
      new MetadataResponse.TopicMetadata(Errors.TOPIC_AUTHORIZATION_FAILED, topic, common.Topic.isInternal(topic),
        java.util.Collections.emptyList()))

    // In version 0, we returned an error when brokers with replicas were unavailable,
    // while in higher versions we simply don't include the broker in the returned broker list
    val errorUnavailableEndpoints = requestVersion == 0
    val topicMetadata =
      if (authorizedTopics.isEmpty)
        Seq.empty[MetadataResponse.TopicMetadata]
      else
        getTopicMetadata(authorizedTopics, request.securityProtocol, errorUnavailableEndpoints)

    val completeTopicMetadata = topicMetadata ++ unauthorizedTopicMetadata

    val brokers = metadataCache.getAliveBrokers

    trace("Sending topic metadata %s and brokers %s for correlation id %d to client %s".format(completeTopicMetadata.mkString(","),
      brokers.mkString(","), request.header.correlationId, request.header.clientId))

    val responseHeader = new ResponseHeader(request.header.correlationId)

    val responseBody = new MetadataResponse(
      brokers.map(_.getNode(request.securityProtocol)).asJava,
      metadataCache.getControllerId.getOrElse(MetadataResponse.NO_CONTROLLER_ID),
      completeTopicMetadata.asJava,
      requestVersion
    )
    requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, responseHeader, responseBody)))
  }

  /*
   * Handle an offset fetch request
   */
  // 处理OFFSET_FETCH请求
  def handleOffsetFetchRequest(request: RequestChannel.Request) {
    val header = request.header
    // 获取请求
    val offsetFetchRequest = request.body.asInstanceOf[OffsetFetchRequest]

    val responseHeader = new ResponseHeader(header.correlationId)
    val offsetFetchResponse =
    // reject the request if not authorized to the group
    if (!authorize(request.session, Read, new Resource(Group, offsetFetchRequest.groupId))) {
      val unauthorizedGroupResponse = new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET, "", Errors.GROUP_AUTHORIZATION_FAILED.code)
      val results = offsetFetchRequest.partitions.asScala.map { topicPartition => (topicPartition, unauthorizedGroupResponse)}.toMap
      new OffsetFetchResponse(results.asJava)
    } else {
      val (authorizedTopicPartitions, unauthorizedTopicPartitions) = offsetFetchRequest.partitions.asScala.partition { topicPartition =>
        authorize(request.session, Describe, new Resource(Topic, topicPartition.topic))
      }
      val unauthorizedTopicResponse = new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET, "", Errors.TOPIC_AUTHORIZATION_FAILED.code)
      val unauthorizedStatus = unauthorizedTopicPartitions.map(topicPartition => (topicPartition, unauthorizedTopicResponse)).toMap
      val unknownTopicPartitionResponse = new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET, "", Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
      // 如果apiVersion == 0那么记录存储在zk中，所以需要从zk拉取
      if (header.apiVersion == 0) {
        // version 0 reads offsets from ZK
        val responseInfo = authorizedTopicPartitions.map { topicPartition =>
          val topicDirs = new ZKGroupTopicDirs(offsetFetchRequest.groupId, topicPartition.topic)
          try {
            if (!metadataCache.hasTopicMetadata(topicPartition.topic))
              (topicPartition, unknownTopicPartitionResponse)
            else {
              val payloadOpt = zkUtils.readDataMaybeNull(s"${topicDirs.consumerOffsetDir}/${topicPartition.partition}")._1
              payloadOpt match {
                case Some(payload) =>
                  (topicPartition, new OffsetFetchResponse.PartitionData(payload.toLong, "", Errors.NONE.code))
                case None =>
                  (topicPartition, unknownTopicPartitionResponse)
              }
            }
          } catch {
            case e: Throwable =>
              (topicPartition, new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET, "",
                Errors.forException(e).code))
          }
        }.toMap
        new OffsetFetchResponse((responseInfo ++ unauthorizedStatus).asJava)
      // apiVersion != 0，消息是存储到了__consumer_offsets某个分区中
      } else {
        // version 1 reads offsets from Kafka;
        // 获取请求中所有的topic-partition的偏移量
        val offsets = coordinator.handleFetchOffsets(offsetFetchRequest.groupId, authorizedTopicPartitions).toMap

        // Note that we do not need to filter the partitions in the
        // metadata cache as the topic partitions will be filtered
        // in coordinator's offset manager through the offset cache
        // 返回响应
        new OffsetFetchResponse((offsets ++ unauthorizedStatus).asJava)
      }
    }

    trace(s"Sending offset fetch response $offsetFetchResponse for correlation id ${header.correlationId} to client ${header.clientId}.")
    requestChannel.sendResponse(new Response(request, new ResponseSend(request.connectionId, responseHeader, offsetFetchResponse)))
  }

  /**
    * 处理GROUP_COORDINATOR请求
    * 获取可用的Coordinator
    * @param request
    */
  def handleGroupCoordinatorRequest(request: RequestChannel.Request) {
    val groupCoordinatorRequest = request.body.asInstanceOf[GroupCoordinatorRequest]
    val responseHeader = new ResponseHeader(request.header.correlationId)
    // 验证权限
    if (!authorize(request.session, Describe, new Resource(Group, groupCoordinatorRequest.groupId))) {
      val responseBody = new GroupCoordinatorResponse(Errors.GROUP_AUTHORIZATION_FAILED.code, Node.noNode)
      requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, responseHeader, responseBody)))
    } else {
      // 计算当前consumer所属组要存储到"__consumer_offsets"的那一个分区上
      val partition = coordinator.partitionFor(groupCoordinatorRequest.groupId)

      // get metadata (and create the topic if necessary)
      // 获取"__consumer_offsets" topic的所有分区元数据
      val offsetsTopicMetadata = getOrCreateGroupMetadataTopic(request.securityProtocol)
      // 有异常不继承处理，抛出错误给consumer
      val responseBody = if (offsetsTopicMetadata.error != Errors.NONE) {
        new GroupCoordinatorResponse(Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code, Node.noNode)
      } else {
        // 从"__consumer_offsets"所有分区中查找上述计算出的分区的Leader所在Broker
        val coordinatorEndpoint: Option[Node] = offsetsTopicMetadata.partitionMetadata().asScala
          .find(_.partition == partition)
          .map(_.leader())

        // 返回Node作为consumer的GroupCoordinator节点
        coordinatorEndpoint match {
          case Some(endpoint) if !endpoint.isEmpty =>
            new GroupCoordinatorResponse(Errors.NONE.code, endpoint)
          case _ =>
            new GroupCoordinatorResponse(Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code, Node.noNode)
        }
      }

      trace("Sending consumer metadata %s for correlation id %d to client %s."
        .format(responseBody, request.header.correlationId, request.header.clientId))
      requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, responseHeader, responseBody)))
    }
  }

  def handleDescribeGroupRequest(request: RequestChannel.Request) {
    val describeRequest = request.body.asInstanceOf[DescribeGroupsRequest]
    val responseHeader = new ResponseHeader(request.header.correlationId)

    val groups = describeRequest.groupIds().asScala.map {
      case groupId =>
        if (!authorize(request.session, Describe, new Resource(Group, groupId))) {
          groupId -> DescribeGroupsResponse.GroupMetadata.forError(Errors.GROUP_AUTHORIZATION_FAILED)
        } else {
          val (error, summary) = coordinator.handleDescribeGroup(groupId)
          val members = summary.members.map { member =>
            val metadata = ByteBuffer.wrap(member.metadata)
            val assignment = ByteBuffer.wrap(member.assignment)
            new DescribeGroupsResponse.GroupMember(member.memberId, member.clientId, member.clientHost, metadata, assignment)
          }
          groupId -> new DescribeGroupsResponse.GroupMetadata(error.code, summary.state, summary.protocolType,
            summary.protocol, members.asJava)
        }
    }.toMap

    val responseBody = new DescribeGroupsResponse(groups.asJava)
    requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, responseHeader, responseBody)))
  }

  def handleListGroupsRequest(request: RequestChannel.Request) {
    val responseHeader = new ResponseHeader(request.header.correlationId)
    val responseBody = if (!authorize(request.session, Describe, Resource.ClusterResource)) {
      ListGroupsResponse.fromError(Errors.CLUSTER_AUTHORIZATION_FAILED)
    } else {
      val (error, groups) = coordinator.handleListGroups()
      val allGroups = groups.map { group => new ListGroupsResponse.Group(group.groupId, group.protocolType) }
      new ListGroupsResponse(error.code, allGroups.asJava)
    }
    requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, responseHeader, responseBody)))
  }

  /**
    * 处理JOIN_GROUP请求
    * consumer加入组
    * @param request
    */
  def handleJoinGroupRequest(request: RequestChannel.Request) {
    import JavaConversions._

    val joinGroupRequest = request.body.asInstanceOf[JoinGroupRequest]
    val responseHeader = new ResponseHeader(request.header.correlationId)

    // the callback for sending a join-group response
    // 回调方法，将结果返回给consumer
    def sendResponseCallback(joinResult: JoinGroupResult) {
      val members = joinResult.members map { case (memberId, metadataArray) => (memberId, ByteBuffer.wrap(metadataArray)) }
      val responseBody = new JoinGroupResponse(joinResult.errorCode, joinResult.generationId, joinResult.subProtocol,
        joinResult.memberId, joinResult.leaderId, members)

      trace("Sending join group response %s for correlation id %d to client %s."
        .format(responseBody, request.header.correlationId, request.header.clientId))
      requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, responseHeader, responseBody)))
    }

    if (!authorize(request.session, Read, new Resource(Group, joinGroupRequest.groupId()))) {
      val responseBody = new JoinGroupResponse(
        Errors.GROUP_AUTHORIZATION_FAILED.code,
        JoinGroupResponse.UNKNOWN_GENERATION_ID,
        JoinGroupResponse.UNKNOWN_PROTOCOL,
        JoinGroupResponse.UNKNOWN_MEMBER_ID, // memberId
        JoinGroupResponse.UNKNOWN_MEMBER_ID, // leaderId
        Map.empty[String, ByteBuffer])
      requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, responseHeader, responseBody)))
    } else {
      // let the coordinator to handle join-group
      // 获取List<ProtocolMetadata>并转为元组(assignor.name(),List<Subscription>)
      // ProtocolMetadata中name为assignor.name()、metadata为Subscription
      val protocols = joinGroupRequest.groupProtocols().map(protocol =>
        (protocol.name, Utils.toArray(protocol.metadata))).toList
      // 处理加入组
      coordinator.handleJoinGroup(
        joinGroupRequest.groupId,
        joinGroupRequest.memberId,
        request.header.clientId,
        request.session.clientAddress.toString,
        joinGroupRequest.sessionTimeout,
        joinGroupRequest.protocolType,
        protocols,
        sendResponseCallback)
    }
  }

  // 处理SYNC_GROUP请求
  def handleSyncGroupRequest(request: RequestChannel.Request) {
    import JavaConversions._

    val syncGroupRequest = request.body.asInstanceOf[SyncGroupRequest]
    // 回调方法用来发送响应
    def sendResponseCallback(memberState: Array[Byte], errorCode: Short) {
      val responseBody = new SyncGroupResponse(errorCode, ByteBuffer.wrap(memberState))
      val responseHeader = new ResponseHeader(request.header.correlationId)
      requestChannel.sendResponse(new Response(request, new ResponseSend(request.connectionId, responseHeader, responseBody)))
    }

    if (!authorize(request.session, Read, new Resource(Group, syncGroupRequest.groupId()))) {
      sendResponseCallback(Array[Byte](), Errors.GROUP_AUTHORIZATION_FAILED.code)
    } else {
      coordinator.handleSyncGroup(
        syncGroupRequest.groupId(),
        syncGroupRequest.generationId(),
        syncGroupRequest.memberId(),
        syncGroupRequest.groupAssignment().mapValues(Utils.toArray(_)),
        sendResponseCallback
      )
    }
  }

  def handleHeartbeatRequest(request: RequestChannel.Request) {
    val heartbeatRequest = request.body.asInstanceOf[HeartbeatRequest]
    val respHeader = new ResponseHeader(request.header.correlationId)

    // the callback for sending a heartbeat response
    def sendResponseCallback(errorCode: Short) {
      val response = new HeartbeatResponse(errorCode)
      trace("Sending heartbeat response %s for correlation id %d to client %s."
        .format(response, request.header.correlationId, request.header.clientId))
      requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, respHeader, response)))
    }

    if (!authorize(request.session, Read, new Resource(Group, heartbeatRequest.groupId))) {
      val heartbeatResponse = new HeartbeatResponse(Errors.GROUP_AUTHORIZATION_FAILED.code)
      requestChannel.sendResponse(new Response(request, new ResponseSend(request.connectionId, respHeader, heartbeatResponse)))
    }
    else {
      // let the coordinator to handle heartbeat
      coordinator.handleHeartbeat(
        heartbeatRequest.groupId(),
        heartbeatRequest.memberId(),
        heartbeatRequest.groupGenerationId(),
        sendResponseCallback)
    }
  }

  /*
   * Returns a Map of all quota managers configured. The request Api key is the key for the Map
   */
  private def instantiateQuotaManagers(cfg: KafkaConfig): Map[Short, ClientQuotaManager] = {
    val producerQuotaManagerCfg = ClientQuotaManagerConfig(
      quotaBytesPerSecondDefault = cfg.producerQuotaBytesPerSecondDefault,
      numQuotaSamples = cfg.numQuotaSamples,
      quotaWindowSizeSeconds = cfg.quotaWindowSizeSeconds
    )

    val consumerQuotaManagerCfg = ClientQuotaManagerConfig(
      quotaBytesPerSecondDefault = cfg.consumerQuotaBytesPerSecondDefault,
      numQuotaSamples = cfg.numQuotaSamples,
      quotaWindowSizeSeconds = cfg.quotaWindowSizeSeconds
    )

    val quotaManagers = Map[Short, ClientQuotaManager](
      ApiKeys.PRODUCE.id ->
              new ClientQuotaManager(producerQuotaManagerCfg, metrics, ApiKeys.PRODUCE.name, new org.apache.kafka.common.utils.SystemTime),
      ApiKeys.FETCH.id ->
              new ClientQuotaManager(consumerQuotaManagerCfg, metrics, ApiKeys.FETCH.name, new org.apache.kafka.common.utils.SystemTime)
    )
    quotaManagers
  }
  // 处理LEAVE_GROUP请求
  def handleLeaveGroupRequest(request: RequestChannel.Request) {
    val leaveGroupRequest = request.body.asInstanceOf[LeaveGroupRequest]
    val respHeader = new ResponseHeader(request.header.correlationId)

    // the callback for sending a leave-group response
    def sendResponseCallback(errorCode: Short) {
      val response = new LeaveGroupResponse(errorCode)
      trace("Sending leave group response %s for correlation id %d to client %s."
                    .format(response, request.header.correlationId, request.header.clientId))
      requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, respHeader, response)))
    }

    if (!authorize(request.session, Read, new Resource(Group, leaveGroupRequest.groupId))) {
      val leaveGroupResponse = new LeaveGroupResponse(Errors.GROUP_AUTHORIZATION_FAILED.code)
      requestChannel.sendResponse(new Response(request, new ResponseSend(request.connectionId, respHeader, leaveGroupResponse)))
    } else {
      // let the coordinator to handle leave-group
      coordinator.handleLeaveGroup(
        leaveGroupRequest.groupId(),
        leaveGroupRequest.memberId(),
        sendResponseCallback)
    }
  }

  def handleSaslHandshakeRequest(request: RequestChannel.Request) {
    val respHeader = new ResponseHeader(request.header.correlationId)
    val response = new SaslHandshakeResponse(Errors.ILLEGAL_SASL_STATE.code, config.saslEnabledMechanisms)
    requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, respHeader, response)))
  }

  def handleApiVersionsRequest(request: RequestChannel.Request) {
    // Note that broker returns its full list of supported ApiKeys and versions regardless of current
    // authentication state (e.g., before SASL authentication on an SASL listener, do note that no
    // Kafka protocol requests may take place on a SSL listener before the SSL handshake is finished).
    // If this is considered to leak information about the broker version a workaround is to use SSL
    // with client authentication which is performed at an earlier stage of the connection where the
    // ApiVersionRequest is not available.
    val responseHeader = new ResponseHeader(request.header.correlationId)
    val responseBody = if (Protocol.apiVersionSupported(ApiKeys.API_VERSIONS.id, request.header.apiVersion))
      ApiVersionsResponse.apiVersionsResponse
    else
      ApiVersionsResponse.fromError(Errors.UNSUPPORTED_VERSION)
    requestChannel.sendResponse(new RequestChannel.Response(request, new ResponseSend(request.connectionId, responseHeader, responseBody)))
  }

  def close() {
    quotaManagers.foreach { case (apiKey, quotaManager) =>
      quotaManager.shutdown()
    }
    info("Shutdown complete.")
  }

  def authorizeClusterAction(request: RequestChannel.Request): Unit = {
    if (!authorize(request.session, ClusterAction, Resource.ClusterResource))
      throw new ClusterAuthorizationException(s"Request $request is not authorized.")
  }
}
