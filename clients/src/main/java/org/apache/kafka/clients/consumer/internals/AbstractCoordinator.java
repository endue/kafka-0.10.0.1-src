/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.GroupCoordinatorNotAvailableException;
import org.apache.kafka.common.errors.IllegalGenerationException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.GroupCoordinatorRequest;
import org.apache.kafka.common.requests.GroupCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupRequest.ProtocolMetadata;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * AbstractCoordinator implements group management for a single group member by interacting with
 * a designated Kafka broker (the coordinator). Group semantics are provided by extending this class.
 * See {@link ConsumerCoordinator} for example usage.
 *
 * From a high level, Kafka's group management protocol consists of the following sequence of actions:
 *
 * <ol>
 *     <li>Group Registration: Group members register with the coordinator providing their own metadata
 *         (such as the set of topics they are interested in).</li>
 *     <li>Group/Leader Selection: The coordinator select the members of the group and chooses one member
 *         as the leader.</li>
 *     <li>State Assignment: The leader collects the metadata from all the members of the group and
 *         assigns state.</li>
 *     <li>Group Stabilization: Each member receives the state assigned by the leader and begins
 *         processing.</li>
 * </ol>
 *
 * To leverage this protocol, an implementation must define the format of metadata provided by each
 * member for group registration in {@link #metadata()} and the format of the state assignment provided
 * by the leader in {@link #performAssignment(String, String, Map)} and becomes available to members in
 * {@link #onJoinComplete(int, String, String, ByteBuffer)}.
 *
 */
public abstract class AbstractCoordinator implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(AbstractCoordinator.class);
    // 心跳类
    private final Heartbeat heartbeat;
    // 心跳任务
    private final HeartbeatTask heartbeatTask;
    // 会话超时时间，超过这个时间没有心跳没有数据交换就关闭连接
    private final int sessionTimeoutMs;
    private final GroupCoordinatorMetrics sensors;
    // 组ID
    protected final String groupId;
    protected final ConsumerNetworkClient client;
    protected final Time time;
    // GroupCoordinator重试阻塞时间
    protected final long retryBackoffMs;

    private boolean needsJoinPrepare = true;
    // 标识是否需要重新加入组
    // 当join group后该值为false
    // 分配分区失败后该值为false
    private boolean rejoinNeeded = true;
    // 收到GROUP_COORDINATOR请求响应后，初始化该值
    protected Node coordinator;
    // 成员ID，默认UNKNOWN_MEMBER_ID，当join group后该值变有效
    protected String memberId;
    // 当join group后返回的协议
    protected String protocol;
    // 当join group后返回的代
    protected int generation;

    /**
     * Initialize the coordination manager.
     */
    public AbstractCoordinator(ConsumerNetworkClient client,
                               String groupId,
                               int sessionTimeoutMs,
                               int heartbeatIntervalMs,// 心跳间隔时间
                               Metrics metrics,
                               String metricGrpPrefix,
                               Time time,
                               long retryBackoffMs) {
        this.client = client;
        this.time = time;
        this.generation = OffsetCommitRequest.DEFAULT_GENERATION_ID;
        this.memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        this.groupId = groupId;
        this.coordinator = null;
        this.sessionTimeoutMs = sessionTimeoutMs;
        // 初始化心跳任务
        this.heartbeat = new Heartbeat(this.sessionTimeoutMs, heartbeatIntervalMs, time.milliseconds());
        // 创建心跳任务
        this.heartbeatTask = new HeartbeatTask();
        this.sensors = new GroupCoordinatorMetrics(metrics, metricGrpPrefix);
        this.retryBackoffMs = retryBackoffMs;
    }

    /**
     * Unique identifier for the class of protocols implements (e.g. "consumer" or "connect").
     * @return Non-null protocol type name
     */
    protected abstract String protocolType();

    /**
     * Get the current list of protocols and their associated metadata supported
     * by the local member. The order of the protocols in the list indicates the preference
     * of the protocol (the first entry is the most preferred). The coordinator takes this
     * preference into account when selecting the generation protocol (generally more preferred
     * protocols will be selected as long as all members support them and there is no disagreement
     * on the preference).
     * @return Non-empty map of supported protocols and metadata
     */
    protected abstract List<ProtocolMetadata> metadata();

    /**
     * Invoked prior to each group join or rejoin. This is typically used to perform any
     * cleanup from the previous generation (such as committing offsets for the consumer)
     * @param generation The previous generation or -1 if there was none
     * @param memberId The identifier of this member in the previous group or "" if there was none
     */
    protected abstract void onJoinPrepare(int generation, String memberId);

    /**
     * Perform assignment for the group. This is used by the leader to push state to all the members
     * of the group (e.g. to push partition assignments in the case of the new consumer)
     * @param leaderId The id of the leader (which is this member)
     * @param allMemberMetadata Metadata from all members of the group
     * @return A map from each member to their state assignment
     */
    protected abstract Map<String, ByteBuffer> performAssignment(String leaderId,
                                                                 String protocol,
                                                                 Map<String, ByteBuffer> allMemberMetadata);

    /**
     * Invoked when a group member has successfully joined a group.
     * @param generation The generation that was joined
     * @param memberId The identifier for the local member in the group
     * @param protocol The protocol selected by the coordinator
     * @param memberAssignment The assignment propagated from the group leader
     */
    protected abstract void onJoinComplete(int generation,
                                           String memberId,
                                           String protocol,
                                           ByteBuffer memberAssignment);

    /**
     * Block until the coordinator for this group is known and is ready to receive requests.
     * 阻塞操作获取一个可用的Coordinator
     */
    public void ensureCoordinatorReady() {
        while (coordinatorUnknown()) {
            // 发送"GROUP_COORDINATOR"消息到配置的"bootstrap.servers"上的某一节点(实际将请求记录到unsent集合中)
            RequestFuture<Void> future = sendGroupCoordinatorRequest();
            // 将unset集合中的请求发送出去，然后阻塞直到future执行完毕
            client.poll(future);
            // 判断异步请求结果是否成功

            // 失败
            if (future.failed()) {
                // 判断请求是否可以重试
                if (future.isRetriable())
                    // 等待元数据更新
                    client.awaitMetadataUpdate();
                else
                    throw future.exception();
            // coordinator不为null && 建立连接失败
            } else if (coordinator != null && client.connectionFailed(coordinator)) {
                // we found the coordinator, but the connection has failed, so mark
                // it dead and backoff before retrying discovery
                // 清空GroupCoordinator之后睡眠等待重试
                coordinatorDead();
                time.sleep(retryBackoffMs);
            }

        }
    }

    /**
     * Check whether the group should be rejoined (e.g. if metadata changes)
     * @return true if it should, false otherwise
     */
    protected boolean needRejoin() {
        return rejoinNeeded;
    }

    /**
     * Ensure that the group is active (i.e. joined and synced)
     * 确保group是可用的
     */
    public void ensureActiveGroup() {
        // 判断“rejoinNeeded”确定是否需要重新加入组
        // 当consumer首次启动或加入组失败或离开当前组等情况下该方法返回true
        if (!needRejoin())
            return;

        //  触发onJoinPrepare, 包括offset commit 和rebalance listener
        if (needsJoinPrepare) {
            onJoinPrepare(generation, memberId);
            needsJoinPrepare = false;
        }

        // 再次判断rejoinNeeded”确定是否需要重新加入组
        while (needRejoin()) {
            // 确保GroupCoordinator已经连接
            ensureCoordinatorReady();

            // ensure that there are no pending requests to the coordinator. This is important
            // in particular to avoid resending a pending JoinGroup request.
            // 如果对当前GroupCoordinator有未发送的消息则等待消息发送完毕
            // 避免重新发起JoinGroup请求
            if (client.pendingRequestCount(this.coordinator) > 0) {
                client.awaitPendingRequests(this.coordinator);
                continue;
            }

            // 发送joinGroup请求
            RequestFuture<ByteBuffer> future = sendJoinGroupRequest();
            future.addListener(new RequestFutureListener<ByteBuffer>() {
                @Override
                public void onSuccess(ByteBuffer value) {
                    // handle join completion in the callback so that the callback will be invoked
                    // even if the consumer is woken up before finishing the rebalance
                    onJoinComplete(generation, memberId, protocol, value);
                    needsJoinPrepare = true;
                    heartbeatTask.reset();
                }

                @Override
                public void onFailure(RuntimeException e) {
                    // we handle failures below after the request finishes. if the join completes
                    // after having been woken up, the exception is ignored and we will rejoin
                }
            });
            // 阻塞等待响应
            client.poll(future);
            // 处理发送失败的异常情况
            if (future.failed()) {
                RuntimeException exception = future.exception();
                if (exception instanceof UnknownMemberIdException ||
                        exception instanceof RebalanceInProgressException ||
                        exception instanceof IllegalGenerationException)
                    continue;
                else if (!future.isRetriable())
                    throw exception;
                time.sleep(retryBackoffMs);
            }
        }
    }

    /**
     * 心跳任务
     */
    private class HeartbeatTask implements DelayedTask {
        // 标记是否有正在发生的心跳任务
        private boolean requestInFlight = false;

        // 重置心跳任务
        public void reset() {
            // start or restart the heartbeat task to be executed at the next chance
            long now = time.milliseconds();
            // 重置lastSessionReset
            heartbeat.resetSessionTimeout(now);
            // 删除任务队列中记录的当前任务
            client.unschedule(this);
            // 如果没有正在发生的心跳任务，那么立即发生一个
            if (!requestInFlight)
                client.schedule(this, now);
        }

        @Override
        public void run(final long now) {
            if (generation < 0 || needRejoin() || coordinatorUnknown()) {
                // no need to send the heartbeat we're not using auto-assignment or if we are
                // awaiting a rebalance
                return;
            }
            // now - Math.max(lastSessionReset, lastHeartbeatReceive) > timeout
            // 认为GroupCoordinator死掉了
            if (heartbeat.sessionTimeoutExpired(now)) {
                // we haven't received a successful heartbeat in one session interval
                // so mark the coordinator dead
                coordinatorDead();
                return;
            }
            // 当前是否需要发送心跳
            if (!heartbeat.shouldHeartbeat(now)) {
                // we don't need to heartbeat now, so reschedule for when we do
                client.schedule(this, now + heartbeat.timeToNextHeartbeat(now));
            } else {
                // 更新lastHeartbeatSend为当前时间戳
                heartbeat.sentHeartbeat(now);
                requestInFlight = true;
                // 发送HEARTBEAT请求
                RequestFuture<Void> future = sendHeartbeatRequest();
                future.addListener(new RequestFutureListener<Void>() {
                    // 处理HEARTBEAT请求的响应
                    @Override
                    public void onSuccess(Void value) {
                        requestInFlight = false;
                        long now = time.milliseconds();
                        // 当收到响应后重置lastHeartbeatReceive
                        heartbeat.receiveHeartbeat(now);
                        // 设置下一次心跳的时间
                        long nextHeartbeatTime = now + heartbeat.timeToNextHeartbeat(now);
                        client.schedule(HeartbeatTask.this, nextHeartbeatTime);
                    }

                    @Override
                    public void onFailure(RuntimeException e) {
                        requestInFlight = false;
                        client.schedule(HeartbeatTask.this, time.milliseconds() + retryBackoffMs);
                    }
                });
            }
        }
    }

    /**
     * Join the group and return the assignment for the next generation. This function handles both
     * JoinGroup and SyncGroup, delegating to {@link #performAssignment(String, String, Map)} if
     * elected leader by the coordinator.
     * @return A request future which wraps the assignment returned from the group leader
     */
    // 发送JOIN_GROUP请求，并处理响应消息
    private RequestFuture<ByteBuffer> sendJoinGroupRequest() {
        if (coordinatorUnknown())
            return RequestFuture.coordinatorNotAvailable();

        // send a join group request to the coordinator
        log.info("(Re-)joining group {}", groupId);
        // 封装JoinGroupRequest
        JoinGroupRequest request = new JoinGroupRequest(
                groupId,
                this.sessionTimeoutMs,
                this.memberId,
                protocolType(),// consumer、dummy、connect
                metadata());// 相对kafkaConsumer来说，这里是ProtocolMetadata集合，ProtocolMetadata内的name为PartitionAssignor的名称，metadata为序列化后的Subscription

        log.debug("Sending JoinGroup ({}) to coordinator {}", request, this.coordinator);
        // 发送JOIN_GROUP请求并处理响应消息
        return client.send(coordinator, ApiKeys.JOIN_GROUP, request)
                // JoinGroupResponseHandler处理回调
                .compose(new JoinGroupResponseHandler());
    }

    // 处理JOIN_GROUP的响应消息
    private class JoinGroupResponseHandler extends CoordinatorResponseHandler<JoinGroupResponse, ByteBuffer> {

        // 解析响应封装为一个JoinGroupResponse
        @Override
        public JoinGroupResponse parse(ClientResponse response) {
            return new JoinGroupResponse(response.responseBody());
        }

        // 处理响应
        @Override
        public void handle(JoinGroupResponse joinResponse, RequestFuture<ByteBuffer> future) {
            Errors error = Errors.forCode(joinResponse.errorCode());
            if (error == Errors.NONE) {
                log.debug("Received successful join group response for group {}: {}", groupId, joinResponse.toStruct());
                AbstractCoordinator.this.memberId = joinResponse.memberId();
                AbstractCoordinator.this.generation = joinResponse.generationId();
                AbstractCoordinator.this.rejoinNeeded = false;
                AbstractCoordinator.this.protocol = joinResponse.groupProtocol();
                sensors.joinLatency.record(response.requestLatencyMs());
                // 当前的kafkaConsumer是leader，分配分区，然后调用参数future中的方法
                if (joinResponse.isLeader()) {
                    onJoinLeader(joinResponse).chain(future);
                // 当前的kafkaConsumer是follower，然后调用参数future中的方法
                } else {
                    onJoinFollower().chain(future);
                }
            } else if (error == Errors.GROUP_LOAD_IN_PROGRESS) {
                log.debug("Attempt to join group {} rejected since coordinator {} is loading the group.", groupId,
                        coordinator);
                // backoff and retry
                future.raise(error);
            } else if (error == Errors.UNKNOWN_MEMBER_ID) {
                // reset the member id and retry immediately
                AbstractCoordinator.this.memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
                log.debug("Attempt to join group {} failed due to unknown member id.", groupId);
                future.raise(Errors.UNKNOWN_MEMBER_ID);
            } else if (error == Errors.GROUP_COORDINATOR_NOT_AVAILABLE
                    || error == Errors.NOT_COORDINATOR_FOR_GROUP) {
                // re-discover the coordinator and retry with backoff
                coordinatorDead();
                log.debug("Attempt to join group {} failed due to obsolete coordinator information: {}", groupId, error.message());
                future.raise(error);
            } else if (error == Errors.INCONSISTENT_GROUP_PROTOCOL
                    || error == Errors.INVALID_SESSION_TIMEOUT
                    || error == Errors.INVALID_GROUP_ID) {
                // log the error and re-throw the exception
                log.error("Attempt to join group {} failed due to fatal error: {}", groupId, error.message());
                future.raise(error);
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                future.raise(new GroupAuthorizationException(groupId));
            } else {
                // unexpected error, throw the exception
                future.raise(new KafkaException("Unexpected error in join group response: " + error.message()));
            }
        }
    }

    // 当前的kafkaConsumer是follower
    private RequestFuture<ByteBuffer> onJoinFollower() {
        // send follower's sync group with an empty assignment
        // 封装一个SyncGroupRequest
        SyncGroupRequest request = new SyncGroupRequest(groupId, generation,
                memberId, Collections.<String, ByteBuffer>emptyMap());
        log.debug("Sending follower SyncGroup for group {} to coordinator {}: {}", groupId, this.coordinator, request);
        // 发送SyncGroupRequest请求
        return sendSyncGroupRequest(request);
    }

    // 当join group后当前consumer成为leader需要为组成员分配分区
    private RequestFuture<ByteBuffer> onJoinLeader(JoinGroupResponse joinResponse) {
        try {
            // perform the leader synchronization and send back the assignment for the group
            // 完成任务分配的工作
            Map<String, ByteBuffer> groupAssignment = performAssignment(joinResponse.leaderId(), joinResponse.groupProtocol(),
                    joinResponse.members());
            // 将分配结果发生给GroupCoordinator
            SyncGroupRequest request = new SyncGroupRequest(groupId, generation, memberId, groupAssignment);
            log.debug("Sending leader SyncGroup for group {} to coordinator {}: {}", groupId, this.coordinator, request);
            return sendSyncGroupRequest(request);
        } catch (RuntimeException e) {
            return RequestFuture.failure(e);
        }
    }

    // 发送SYNC_GROUP
    private RequestFuture<ByteBuffer> sendSyncGroupRequest(SyncGroupRequest request) {
        if (coordinatorUnknown())
            return RequestFuture.coordinatorNotAvailable();
        return client.send(coordinator, ApiKeys.SYNC_GROUP, request)
                .compose(new SyncGroupResponseHandler());
    }

    // 处理SYNC_GROUP的响应消息
    private class SyncGroupResponseHandler extends CoordinatorResponseHandler<SyncGroupResponse, ByteBuffer> {

        // 解析响应
        @Override
        public SyncGroupResponse parse(ClientResponse response) {
            return new SyncGroupResponse(response.responseBody());
        }

        // 处理响应
        @Override
        public void handle(SyncGroupResponse syncResponse,
                           RequestFuture<ByteBuffer> future) {
            Errors error = Errors.forCode(syncResponse.errorCode());
            // 响应结果正常
            if (error == Errors.NONE) {
                log.info("Successfully joined group {} with generation {}", groupId, generation);
                sensors.syncLatency.record(response.requestLatencyMs());
                // 处理分配结果，这参数被序列后的Assignment对象
                future.complete(syncResponse.memberAssignment());
            } else {
                // 响应结果异常
                AbstractCoordinator.this.rejoinNeeded = true;
                if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                    future.raise(new GroupAuthorizationException(groupId));
                } else if (error == Errors.REBALANCE_IN_PROGRESS) {
                    log.debug("SyncGroup for group {} failed due to coordinator rebalance", groupId);
                    future.raise(error);
                } else if (error == Errors.UNKNOWN_MEMBER_ID
                        || error == Errors.ILLEGAL_GENERATION) {
                    log.debug("SyncGroup for group {} failed due to {}", groupId, error);
                    AbstractCoordinator.this.memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
                    future.raise(error);
                } else if (error == Errors.GROUP_COORDINATOR_NOT_AVAILABLE
                        || error == Errors.NOT_COORDINATOR_FOR_GROUP) {
                    log.debug("SyncGroup for group {} failed due to {}", groupId, error);
                    coordinatorDead();
                    future.raise(error);
                } else {
                    future.raise(new KafkaException("Unexpected error from SyncGroup: " + error.message()));
                }
            }
        }
    }

    /**
     * Discover the current coordinator for the group. Sends a GroupMetadata request to
     * one of the brokers. The returned future should be polled to get the result of the request.
     * @return A request future which indicates the completion of the metadata request
     */
    // 发送GroupCoordinator
    // 每个ConsumerGroup都有其对应的GroupCoordinator，但具体是由哪个GroupCoordinator来负责这与group.id的hash值有关，
    // 通过这个abs(GroupId.hashCode()) % NumPartitions来计算出一个值(其中，NumPartitions 是 __consumer_offsets 的 partition 数，默认是50个)，
    // 这个值代表了__consumer_offsets的一个partition，而这个partition的leader即为这个ConsumerGroup要交互的GroupCoordinator所在的节点
    private RequestFuture<Void> sendGroupCoordinatorRequest() {
        // initiate the group metadata request
        // find a node to ask about the coordinator
        // 获取"bootstrap.servers"上的任一节点
        Node node = this.client.leastLoadedNode();
        // 没有可用节点
        if (node == null) {
            // TODO: If there are no brokers left, perhaps we should use the bootstrap set
            // from configuration?
            return RequestFuture.noBrokersAvailable();
        } else {
            // create a group  metadata request
            log.debug("Sending coordinator request for group {} to broker {}", groupId, node);
            // 构建GROUP_COORDINATOR请求
            GroupCoordinatorRequest metadataRequest = new GroupCoordinatorRequest(this.groupId);
            // 发送请求(实际只是将请求记录到了unsent集合中)
            return client.send(node, ApiKeys.GROUP_COORDINATOR, metadataRequest)
                    .compose(new RequestFutureAdapter<ClientResponse, Void>() {
                        @Override
                        public void onSuccess(ClientResponse response, RequestFuture<Void> future) {
                            // 处理响应
                            handleGroupMetadataResponse(response, future);
                        }
                    });
        }
    }

    /**
     * 处理GROUP_COORDINATOR的响应消息
     */
    private void handleGroupMetadataResponse(ClientResponse resp, RequestFuture<Void> future) {
        log.debug("Received group coordinator response {}", resp);
        // 如果已经存在GroupCoordinator则忽略本次的信息
        if (!coordinatorUnknown()) {
            // We already found the coordinator, so ignore the request
            future.complete(null);
        // 处理CoordinatorResponse
        } else {
            // 获取响应
            GroupCoordinatorResponse groupCoordinatorResponse = new GroupCoordinatorResponse(resp.responseBody());
            // use MAX_VALUE - node.id as the coordinator id to mimic separate connections
            // for the coordinator in the underlying network client layer
            // TODO: this needs to be better handled in KAFKA-1935
            Errors error = Errors.forCode(groupCoordinatorResponse.errorCode());
            // 响应没有异常
            if (error == Errors.NONE) {
                // 构建GroupCoordinator
                this.coordinator = new Node(Integer.MAX_VALUE - groupCoordinatorResponse.node().id(),
                        groupCoordinatorResponse.node().host(),
                        groupCoordinatorResponse.node().port());

                log.info("Discovered coordinator {} for group {}.", coordinator, groupId);
                // 尝试与GroupCoordinator建立socket连接
                client.tryConnect(coordinator);

                // start sending heartbeats only if we have a valid generation
                // 判断是否为有效的"代"
                if (generation > 0)
                    // 重置心跳任务并立即发生一个心跳
                    heartbeatTask.reset();
                future.complete(null);
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                future.raise(new GroupAuthorizationException(groupId));
            } else {
                future.raise(error);
            }
        }
    }

    /**
     * Check if we know who the coordinator is and we have an active connection
     * @return true if the coordinator is unknown
     * 判断GroupCoordinator是否可用
     */
    public boolean coordinatorUnknown() {
        if (coordinator == null)
            return true;
        // GroupCoordinator不为null但是状态为DISCONNECTED的
        if (client.connectionFailed(coordinator)) {
            // 删除发往该GroupCoordinator的消息
            coordinatorDead();
            return true;
        }

        return false;
    }

    /**
     * Mark the current coordinator as dead.
     * GroupCoordinator已失效，清空GroupCoordinator
     */
    protected void coordinatorDead() {
        if (this.coordinator != null) {
            log.info("Marking the coordinator {} dead for group {}", this.coordinator, groupId);
            client.failUnsentRequests(this.coordinator, GroupCoordinatorNotAvailableException.INSTANCE);
            this.coordinator = null;
        }
    }

    /**
     * Close the coordinator, waiting if needed to send LeaveGroup.
     */
    @Override
    public void close() {
        // we do not need to re-enable wakeups since we are closing already
        client.disableWakeups();
        maybeLeaveGroup();
    }

    /**
     * Leave the current group and reset local generation/memberId.
     * 离开当前所处的group，重置本地generation/memberId
     */
    public void maybeLeaveGroup() {
        client.unschedule(heartbeatTask);
        if (!coordinatorUnknown() && generation > 0) {
            // this is a minimal effort attempt to leave the group. we do not
            // attempt any resending if the request fails or times out.
            sendLeaveGroupRequest();
        }

        this.generation = OffsetCommitRequest.DEFAULT_GENERATION_ID;
        this.memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        rejoinNeeded = true;
    }

    private void sendLeaveGroupRequest() {
        LeaveGroupRequest request = new LeaveGroupRequest(groupId, memberId);
        RequestFuture<Void> future = client.send(coordinator, ApiKeys.LEAVE_GROUP, request)
                .compose(new LeaveGroupResponseHandler());

        future.addListener(new RequestFutureListener<Void>() {
            @Override
            public void onSuccess(Void value) {}

            @Override
            public void onFailure(RuntimeException e) {
                log.debug("LeaveGroup request for group {} failed with error", groupId, e);
            }
        });

        client.poll(future, 0);
    }

    private class LeaveGroupResponseHandler extends CoordinatorResponseHandler<LeaveGroupResponse, Void> {
        @Override
        public LeaveGroupResponse parse(ClientResponse response) {
            return new LeaveGroupResponse(response.responseBody());
        }

        @Override
        public void handle(LeaveGroupResponse leaveResponse, RequestFuture<Void> future) {
            // process the response
            short errorCode = leaveResponse.errorCode();
            if (errorCode == Errors.NONE.code())
                future.complete(null);
            else
                future.raise(Errors.forCode(errorCode));
        }
    }

    /**
     * Send a heartbeat request now (visible only for testing).
     */
    // 发送HEARTBEAT心跳
    public RequestFuture<Void> sendHeartbeatRequest() {
        HeartbeatRequest req = new HeartbeatRequest(this.groupId, this.generation, this.memberId);
        return client.send(coordinator, ApiKeys.HEARTBEAT, req)
                .compose(new HeartbeatCompletionHandler());
    }
    // 处理HEARTBEAT的响应消息
    private class HeartbeatCompletionHandler extends CoordinatorResponseHandler<HeartbeatResponse, Void> {
        @Override
        public HeartbeatResponse parse(ClientResponse response) {
            return new HeartbeatResponse(response.responseBody());
        }

        @Override
        public void handle(HeartbeatResponse heartbeatResponse, RequestFuture<Void> future) {
            sensors.heartbeatLatency.record(response.requestLatencyMs());
            Errors error = Errors.forCode(heartbeatResponse.errorCode());
            if (error == Errors.NONE) {
                log.debug("Received successful heartbeat response for group {}", groupId);
                future.complete(null);
            } else if (error == Errors.GROUP_COORDINATOR_NOT_AVAILABLE
                    || error == Errors.NOT_COORDINATOR_FOR_GROUP) {
                log.debug("Attempt to heart beat failed for group {} since coordinator {} is either not started or not valid.",
                        groupId, coordinator);
                coordinatorDead();
                future.raise(error);
            } else if (error == Errors.REBALANCE_IN_PROGRESS) {
                log.debug("Attempt to heart beat failed for group {} since it is rebalancing.", groupId);
                AbstractCoordinator.this.rejoinNeeded = true;
                future.raise(Errors.REBALANCE_IN_PROGRESS);
            } else if (error == Errors.ILLEGAL_GENERATION) {
                log.debug("Attempt to heart beat failed for group {} since generation id is not legal.", groupId);
                AbstractCoordinator.this.rejoinNeeded = true;
                future.raise(Errors.ILLEGAL_GENERATION);
            } else if (error == Errors.UNKNOWN_MEMBER_ID) {
                log.debug("Attempt to heart beat failed for group {} since member id is not valid.", groupId);
                memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
                AbstractCoordinator.this.rejoinNeeded = true;
                future.raise(Errors.UNKNOWN_MEMBER_ID);
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                future.raise(new GroupAuthorizationException(groupId));
            } else {
                future.raise(new KafkaException("Unexpected error in heartbeat response: " + error.message()));
            }
        }
    }

    protected abstract class CoordinatorResponseHandler<R, T>
            extends RequestFutureAdapter<ClientResponse, T> {
        protected ClientResponse response;

        public abstract R parse(ClientResponse response);

        public abstract void handle(R response, RequestFuture<T> future);

        @Override
        public void onFailure(RuntimeException e, RequestFuture<T> future) {
            // mark the coordinator as dead
            if (e instanceof DisconnectException)
                coordinatorDead();
            future.raise(e);
        }

        @Override
        public void onSuccess(ClientResponse clientResponse, RequestFuture<T> future) {
            try {
                this.response = clientResponse;
                R responseObj = parse(clientResponse);
                handle(responseObj, future);
            } catch (RuntimeException e) {
                if (!future.isDone())
                    future.raise(e);
            }
        }

    }

    private class GroupCoordinatorMetrics {
        public final Metrics metrics;
        public final String metricGrpName;

        public final Sensor heartbeatLatency;
        public final Sensor joinLatency;
        public final Sensor syncLatency;

        public GroupCoordinatorMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metrics = metrics;
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            this.heartbeatLatency = metrics.sensor("heartbeat-latency");
            this.heartbeatLatency.add(metrics.metricName("heartbeat-response-time-max",
                this.metricGrpName,
                "The max time taken to receive a response to a heartbeat request"), new Max());
            this.heartbeatLatency.add(metrics.metricName("heartbeat-rate",
                this.metricGrpName,
                "The average number of heartbeats per second"), new Rate(new Count()));

            this.joinLatency = metrics.sensor("join-latency");
            this.joinLatency.add(metrics.metricName("join-time-avg",
                    this.metricGrpName,
                    "The average time taken for a group rejoin"), new Avg());
            this.joinLatency.add(metrics.metricName("join-time-max",
                    this.metricGrpName,
                    "The max time taken for a group rejoin"), new Avg());
            this.joinLatency.add(metrics.metricName("join-rate",
                    this.metricGrpName,
                    "The number of group joins per second"), new Rate(new Count()));

            this.syncLatency = metrics.sensor("sync-latency");
            this.syncLatency.add(metrics.metricName("sync-time-avg",
                    this.metricGrpName,
                    "The average time taken for a group sync"), new Avg());
            this.syncLatency.add(metrics.metricName("sync-time-max",
                    this.metricGrpName,
                    "The max time taken for a group sync"), new Avg());
            this.syncLatency.add(metrics.metricName("sync-rate",
                    this.metricGrpName,
                    "The number of group syncs per second"), new Rate(new Count()));

            Measurable lastHeartbeat =
                new Measurable() {
                    public double measure(MetricConfig config, long now) {
                        return TimeUnit.SECONDS.convert(now - heartbeat.lastHeartbeatSend(), TimeUnit.MILLISECONDS);
                    }
                };
            metrics.addMetric(metrics.metricName("last-heartbeat-seconds-ago",
                this.metricGrpName,
                "The number of seconds since the last controller heartbeat"),
                lastHeartbeat);
        }
    }

}
