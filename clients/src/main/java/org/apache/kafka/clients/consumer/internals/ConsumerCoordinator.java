/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor.Subscription;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.internals.TopicConstants;
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
import org.apache.kafka.common.requests.JoinGroupRequest.ProtocolMetadata;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class manages the coordination process with the consumer coordinator.
 */
public final class ConsumerCoordinator extends AbstractCoordinator {

    private static final Logger log = LoggerFactory.getLogger(ConsumerCoordinator.class);
    // 分区分配策略
    private final List<PartitionAssignor> assignors;
    private final Metadata metadata;
    private final ConsumerCoordinatorMetrics sensors;
    // 订阅状态
    private final SubscriptionState subscriptions;
    // 自动提交offset后的回调
    private final OffsetCommitCallback defaultOffsetCommitCallback;
    // 自动提交offset，默认true
    private final boolean autoCommitEnabled;
    // 自动提交task，默认5000一次
    private final AutoCommitTask autoCommitTask;
    // 拦截器
    private final ConsumerInterceptors<?, ?> interceptors;
    private final boolean excludeInternalTopics;

    private MetadataSnapshot metadataSnapshot;
    private MetadataSnapshot assignmentSnapshot;

    /**
     * Initialize the coordination manager.
     */
    public ConsumerCoordinator(ConsumerNetworkClient client,
                               String groupId,// groupID
                               int sessionTimeoutMs,
                               int heartbeatIntervalMs,// 心跳间隔时间
                               List<PartitionAssignor> assignors,
                               Metadata metadata,
                               SubscriptionState subscriptions,
                               Metrics metrics,
                               String metricGrpPrefix,
                               Time time,
                               long retryBackoffMs,
                               OffsetCommitCallback defaultOffsetCommitCallback,
                               boolean autoCommitEnabled,// 自动提交offset，默认true
                               long autoCommitIntervalMs,// 自动提交offset频率，默认5000
                               ConsumerInterceptors<?, ?> interceptors,
                               boolean excludeInternalTopics) {
        super(client,
                groupId,
                sessionTimeoutMs,
                heartbeatIntervalMs,
                metrics,
                metricGrpPrefix,
                time,
                retryBackoffMs);
        this.metadata = metadata;

        this.metadata.requestUpdate();
        this.metadataSnapshot = new MetadataSnapshot(subscriptions, metadata.fetch());
        this.subscriptions = subscriptions;
        this.defaultOffsetCommitCallback = defaultOffsetCommitCallback;
        this.autoCommitEnabled = autoCommitEnabled;
        this.assignors = assignors;

        addMetadataListener();

        if (autoCommitEnabled) {
            this.autoCommitTask = new AutoCommitTask(autoCommitIntervalMs);
            this.autoCommitTask.reschedule();
        } else {
            this.autoCommitTask = null;
        }

        this.sensors = new ConsumerCoordinatorMetrics(metrics, metricGrpPrefix);
        this.interceptors = interceptors;
        this.excludeInternalTopics = excludeInternalTopics;
    }

    // 获取协议类型，
    // 返回 consumer
    @Override
    public String protocolType() {
        return ConsumerProtocol.PROTOCOL_TYPE;
    }

    @Override
    public List<ProtocolMetadata> metadata() {
        List<ProtocolMetadata> metadataList = new ArrayList<>();
        for (PartitionAssignor assignor : assignors) {
            // 这里的Subscription中“userData”为空
            Subscription subscription = assignor.subscription(subscriptions.subscription());
            // 序列化Subscription
            ByteBuffer metadata = ConsumerProtocol.serializeSubscription(subscription);
            // 在封装为一个ProtocolMetadata，其中name为assignor.name()、metadata为Subscription
            metadataList.add(new ProtocolMetadata(assignor.name(), metadata));
        }
        return metadataList;
    }

    private void addMetadataListener() {
        this.metadata.addListener(new Metadata.Listener() {
            @Override
            public void onMetadataUpdate(Cluster cluster) {

                if (subscriptions.hasPatternSubscription()) {

                    Set<String> unauthorizedTopics = new HashSet<String>();
                    for (String topic : cluster.unauthorizedTopics()) {
                        if (filterTopic(topic))
                            unauthorizedTopics.add(topic);
                    }
                    if (!unauthorizedTopics.isEmpty())
                        throw new TopicAuthorizationException(unauthorizedTopics);

                    final List<String> topicsToSubscribe = new ArrayList<>();

                    for (String topic : cluster.topics())
                        if (filterTopic(topic))
                            topicsToSubscribe.add(topic);

                    subscriptions.changeSubscription(topicsToSubscribe);
                    metadata.setTopics(subscriptions.groupSubscription());
                } else if (!cluster.unauthorizedTopics().isEmpty()) {
                    throw new TopicAuthorizationException(new HashSet<>(cluster.unauthorizedTopics()));
                }

                // check if there are any changes to the metadata which should trigger a rebalance
                if (subscriptions.partitionsAutoAssigned()) {
                    MetadataSnapshot snapshot = new MetadataSnapshot(subscriptions, cluster);
                    if (!snapshot.equals(metadataSnapshot)) {
                        metadataSnapshot = snapshot;
                        subscriptions.needReassignment();
                    }
                }

            }
        });
    }

    private boolean filterTopic(String topic) {
        return subscriptions.getSubscribedPattern().matcher(topic).matches() &&
                !(excludeInternalTopics && TopicConstants.INTERNAL_TOPICS.contains(topic));
    }

    private PartitionAssignor lookupAssignor(String name) {
        for (PartitionAssignor assignor : this.assignors) {
            if (assignor.name().equals(name))
                return assignor;
        }
        return null;
    }

    // 处理SYNC_GROUP请求后响应，也就是GroupCoordinator的响应
    // 也就是获取topic的分配结果
    @Override
    protected void onJoinComplete(int generation,
                                  String memberId,
                                  String assignmentStrategy,// 分配策略
                                  ByteBuffer assignmentBuffer) {// 分配结果
        // if we were the assignor, then we need to make sure that there have been no metadata updates
        // since the rebalance begin. Otherwise, we won't rebalance again until the next metadata change
        if (assignmentSnapshot != null && !assignmentSnapshot.equals(metadataSnapshot)) {
            // 更新needsPartitionAssignment为true，表示需要分区重分配
            subscriptions.needReassignment();
            return;
        }

        PartitionAssignor assignor = lookupAssignor(assignmentStrategy);
        if (assignor == null)
            throw new IllegalStateException("Coordinator selected invalid assignment protocol: " + assignmentStrategy);
        // 解析分配结果
        Assignment assignment = ConsumerProtocol.deserializeAssignment(assignmentBuffer);

        // set the flag to refresh last committed offsets
        // 标记needsFetchCommittedOffsets为true
        subscriptions.needRefreshCommits();

        // update partition assignment
        // 更新获取的分区，更新needsPartitionAssignment为false，表示不需要分区重分配
        subscriptions.assignFromSubscribed(assignment.partitions());

        // give the assignor a chance to update internal state based on the received assignment
        // 将分配的结果信息交给PartitionAssignor的onAssignment()来做一些操作
        assignor.onAssignment(assignment);

        // reschedule the auto commit starting from now
        // 如果启动了自动提交commit offset，那么重置自动提交线程
        if (autoCommitEnabled)
            autoCommitTask.reschedule();

        // execute the user's callback after rebalance
        // 执行回调
        ConsumerRebalanceListener listener = subscriptions.listener();
        log.info("Setting newly assigned partitions {} for group {}", subscriptions.assignedPartitions(), groupId);
        try {
            Set<TopicPartition> assigned = new HashSet<>(subscriptions.assignedPartitions());
            listener.onPartitionsAssigned(assigned);
        } catch (WakeupException e) {
            throw e;
        } catch (Exception e) {
            log.error("User provided listener {} for group {} failed on partition assignment",
                    listener.getClass().getName(), groupId, e);
        }
    }

    /**
     * 执行分配
     * @param leaderId The id of the leader (which is this member)
     * @param assignmentStrategy 分配策略
     * @param allSubscriptions 组中所有成员的元数据
     * @return
     */
    @Override
    protected Map<String, ByteBuffer> performAssignment(String leaderId,// leader节点的id
                                                        String assignmentStrategy,// 分配策略
                                                        Map<String, ByteBuffer> allSubscriptions) {// value是序列后的Subscription
        // 获取分配策略
        PartitionAssignor assignor = lookupAssignor(assignmentStrategy);
        if (assignor == null)
            throw new IllegalStateException("Coordinator selected invalid assignment protocol: " + assignmentStrategy);
        // 记录组成员订阅的所有topic
        Set<String> allSubscribedTopics = new HashSet<>();
        // 记录成员Id和对应的Subscription
        Map<String, Subscription> subscriptions = new HashMap<>();
        // 遍历所以组成员的元数据，反序列化为Subscription
        for (Map.Entry<String, ByteBuffer> subscriptionEntry : allSubscriptions.entrySet()) {
            // 获取成员的Subscription
            Subscription subscription = ConsumerProtocol.deserializeSubscription(subscriptionEntry.getValue());
            // 记录成员的ID和Subscription对象
            subscriptions.put(subscriptionEntry.getKey(), subscription);
            // 获取组成员订阅的topics
            allSubscribedTopics.addAll(subscription.topics());
        }

        // the leader will begin watching for changes to any of the topics the group is interested in,
        // which ensures that all metadata changes will eventually be seen
        // 更新groupSubscription字段
        this.subscriptions.groupSubscribe(allSubscribedTopics);
        // 更新元数据
        metadata.setTopics(this.subscriptions.groupSubscription());

        // update metadata (if needed) and keep track of the metadata used for assignment so that
        // we can check after rebalance completion whether anything has changed
        // 更新元数据
        client.ensureFreshMetadata();
        assignmentSnapshot = metadataSnapshot;

        log.debug("Performing assignment for group {} using strategy {} with subscriptions {}",
                groupId, assignor.name(), subscriptions);
        // 基于assignor对所有的topic进行分区分配
        // 返回结果key是memberId，value是Assignment，Assignment里面记录了分配结果
        Map<String, Assignment> assignment = assignor.assign(metadata.fetch(), subscriptions);

        log.debug("Finished assignment for group {}: {}", groupId, assignment);
        // 封装返回结果，key是memberId，value是序列化后的Assignment，是一个ByteBuffer
        Map<String, ByteBuffer> groupAssignment = new HashMap<>();
        for (Map.Entry<String, Assignment> assignmentEntry : assignment.entrySet()) {
            ByteBuffer buffer = ConsumerProtocol.serializeAssignment(assignmentEntry.getValue());
            groupAssignment.put(assignmentEntry.getKey(), buffer);
        }
        // 将分配返回
        return groupAssignment;
    }

    /**
     *
     * @param generation The previous generation or -1 if there was none
     * @param memberId The identifier of this member in the previous group or "" if there was none
     */
    @Override
    protected void onJoinPrepare(int generation, String memberId) {
        // commit offsets prior to rebalance if auto-commit enabled
        // 提交offset到集群
        maybeAutoCommitOffsetsSync();

        // execute the user's callback before rebalance
        ConsumerRebalanceListener listener = subscriptions.listener();
        log.info("Revoking previously assigned partitions {} for group {}", subscriptions.assignedPartitions(), groupId);
        try {
            Set<TopicPartition> revoked = new HashSet<>(subscriptions.assignedPartitions());
            listener.onPartitionsRevoked(revoked);
        } catch (WakeupException e) {
            throw e;
        } catch (Exception e) {
            log.error("User provided listener {} for group {} failed on partition revocation",
                    listener.getClass().getName(), groupId, e);
        }

        assignmentSnapshot = null;
        subscriptions.needReassignment();
    }

    @Override
    public boolean needRejoin() {
        return subscriptions.partitionsAutoAssigned() &&
                (super.needRejoin() || subscriptions.partitionAssignmentNeeded());
    }

    /**
     * Refresh the committed offsets for provided partitions.
     * 是否需要刷新消费消息的偏移量，也就是从GroupCoordinator中拉取topic-partition已经提交的offset
     * 到对应的TopicPartitionState的committed属性中
     */
    public void refreshCommittedOffsetsIfNeeded() {
        // 当consumer被分配分区后，needsFetchCommittedOffsets == true
        if (subscriptions.refreshCommitsNeeded()) {
            // 拉取topic-partition对应的OffsetAndMetadata数据
            Map<TopicPartition, OffsetAndMetadata> offsets = fetchCommittedOffsets(subscriptions.assignedPartitions());
            // 遍历获取到的结果
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                TopicPartition tp = entry.getKey();
                // verify assignment is still active
                // 确保拉取的topic-partition依旧属于当前consumer来管理
                if (subscriptions.isAssigned(tp))
                    // 更新topic-partition对应的TopicPartitionState的committed = entry.getValue()
                    this.subscriptions.committed(tp, entry.getValue());
            }
            // 更新needsFetchCommittedOffsets = false
            this.subscriptions.commitsRefreshed();
        }
    }

    /**
     * Fetch the current committed offsets from the coordinator for a set of partitions.
     * @param partitions The partitions to fetch offsets for
     * @return A map from partition to the committed offset
     */
    // 获取topic-partition的commit offset
    public Map<TopicPartition, OffsetAndMetadata> fetchCommittedOffsets(Set<TopicPartition> partitions) {
        while (true) {
            // 确保已经与GroupCoordinator建立连接
            ensureCoordinatorReady();

            // contact coordinator to fetch committed offsets
            // 创建OFFSET_FETCH请求
            RequestFuture<Map<TopicPartition, OffsetAndMetadata>> future = sendOffsetFetchRequest(partitions);
            // 发送OFFSET_FETCH请求
            client.poll(future);
            // 执行成功，获取结果
            if (future.succeeded())
                return future.value();
            // 执行失败，如果不允许重试，抛出异常
            if (!future.isRetriable())
                throw future.exception();
            // 执行失败，如果可以重试，等待retryBackoffMs时间后重试
            time.sleep(retryBackoffMs);
        }
    }

    /**
     * Ensure that we have a valid partition assignment from the coordinator.
     * 获得有效分区
     */
    public void ensurePartitionAssignment() {
        if (subscriptions.partitionsAutoAssigned()) {
            // Due to a race condition between the initial metadata fetch and the initial rebalance, we need to ensure that
            // the metadata is fresh before joining initially, and then request the metadata update. If metadata update arrives
            // while the rebalance is still pending (for example, when the join group is still inflight), then we will lose
            // track of the fact that we need to rebalance again to reflect the change to the topic subscription. Without
            // ensuring that the metadata is fresh, any metadata update that changes the topic subscriptions and arrives with a
            // rebalance in progress will essentially be ignored. See KAFKA-3949 for the complete description of the problem.
            // 如果是AUTO_PATTERN订阅模式，则需要确保元数据是最新的
            if (subscriptions.hasPatternSubscription())
                client.ensureFreshMetadata();
            // 确保group是可用的
            ensureActiveGroup();
        }
    }

    @Override
    public void close() {
        // we do not need to re-enable wakeups since we are closing already
        client.disableWakeups();
        try {
            maybeAutoCommitOffsetsSync();
        } finally {
            super.close();
        }
    }

    // 异步提交指定topic-partition的offset
    // 参数offsets中记录了topic-partition下一次拉取消息的offset
    public void commitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        // 标识needsFetchCommittedOffsets为true
        // 也就是需要拉取Committed Offsets
        this.subscriptions.needRefreshCommits();
        // 创建OFFSET_COMMIT请求
        RequestFuture<Void> future = sendOffsetCommitRequest(offsets);
        // 设置回调
        final OffsetCommitCallback cb = callback == null ? defaultOffsetCommitCallback : callback;
        future.addListener(new RequestFutureListener<Void>() {
            @Override
            public void onSuccess(Void value) {
                if (interceptors != null)
                    // 如果有拦截器先执行拦截器的onCommit()方法
                    interceptors.onCommit(offsets);
                // 提交offset成功，执行回调
                cb.onComplete(offsets, null);
            }

            @Override
            public void onFailure(RuntimeException e) {
                if (e instanceof RetriableException) {
                    cb.onComplete(offsets, new RetriableCommitFailedException("Commit offsets failed with retriable exception. You should retry committing offsets.", e));
                } else {
                    cb.onComplete(offsets, e);
                }
            }
        });

        // ensure the commit has a chance to be transmitted (without blocking on its completion).
        // Note that commits are treated as heartbeats by the coordinator, so there is no need to
        // explicitly allow heartbeats through delayed task execution.
        client.pollNoWakeup();
    }

    /**
     * Commit offsets synchronously. This method will retry until the commit completes successfully
     * or an unrecoverable error is encountered.
     * @param offsets The offsets to be committed
     * @throws org.apache.kafka.common.errors.AuthorizationException if the consumer is not authorized to the group
     *             or to any of the specified partitions
     * @throws CommitFailedException if an unrecoverable error occurs before the commit can be completed
     */
    public void commitOffsetsSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (offsets.isEmpty())
            return;

        while (true) {
            // 确保Coordinator
            ensureCoordinatorReady();
            // 封装OFFSET_COMMIT请求
            RequestFuture<Void> future = sendOffsetCommitRequest(offsets);
            // 发送OFFSET_COMMIT请求
            client.poll(future);

            if (future.succeeded()) {
                if (interceptors != null)
                    // 调用拦截器的onCommit方法
                    interceptors.onCommit(offsets);
                return;
            }

            if (!future.isRetriable())
                throw future.exception();

            time.sleep(retryBackoffMs);
        }
    }

    /**
     * 自动提交offset延迟任务
     */
    private class AutoCommitTask implements DelayedTask {
        // 每次提交offset的间隔
        private final long interval;

        public AutoCommitTask(long interval) {
            this.interval = interval;
        }
        // 重新制定任务的执行时间
        private void reschedule() {
            client.schedule(this, time.milliseconds() + interval);
        }
        // 重新制定任务的执行时间，任务的执行时间为参数at
        private void reschedule(long at) {
            client.schedule(this, at);
        }
        // 执行定时任务
        public void run(final long now) {
            if (coordinatorUnknown()) {
                log.debug("Cannot auto-commit offsets for group {} since the coordinator is unknown", groupId);
                reschedule(now + retryBackoffMs);
                return;
            }
            // 判断是否需要重新加入组
            if (needRejoin()) {
                // skip the commit when we're rejoining since we'll commit offsets synchronously
                // before the revocation callback is invoked
                reschedule(now + interval);
                return;
            }
            // 提交offset
            commitOffsetsAsync(subscriptions.allConsumed(), new OffsetCommitCallback() {
                // 执行成功后，重新设置下次任务的执行时间
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    if (exception == null) {
                        reschedule(now + interval);
                    } else {
                        log.warn("Auto offset commit failed for group {}: {}", groupId, exception.getMessage());
                        reschedule(now + interval);
                    }
                }
            });
        }
    }

    private void maybeAutoCommitOffsetsSync() {
        if (autoCommitEnabled) {
            try {
                commitOffsetsSync(subscriptions.allConsumed());
            } catch (WakeupException e) {
                // rethrow wakeups since they are triggered by the user
                throw e;
            } catch (Exception e) {
                // consistent with async auto-commit failures, we do not propagate the exception
                log.warn("Auto offset commit failed for group {}: {}", groupId, e.getMessage());
            }
        }
    }

    /**
     * Commit offsets for the specified list of topics and partitions. This is a non-blocking call
     * which returns a request future that can be polled in the case of a synchronous commit or ignored in the
     * asynchronous case.
     *
     * @param offsets The list of offsets per partition that should be committed.
     * @return A request future whose value indicates whether the commit was successful or not
     */
    // 创建提交topic-partition的offset的OFFSET_COMMIT请求
    private RequestFuture<Void> sendOffsetCommitRequest(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (coordinatorUnknown())
            return RequestFuture.coordinatorNotAvailable();

        if (offsets.isEmpty())
            return RequestFuture.voidSuccess();

        // create the offset commit request
        // 创建每个topic-partition对应的PartitionData
        Map<TopicPartition, OffsetCommitRequest.PartitionData> offsetData = new HashMap<>(offsets.size());
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            OffsetAndMetadata offsetAndMetadata = entry.getValue();
            // 为每一个topic-partition创建一个OffsetCommitRequest.PartitionData
            offsetData.put(entry.getKey(), new OffsetCommitRequest.PartitionData(
                    offsetAndMetadata.offset(), offsetAndMetadata.metadata()));
        }
        // 最终的请求
        OffsetCommitRequest req = new OffsetCommitRequest(this.groupId,
                this.generation,
                this.memberId,
                OffsetCommitRequest.DEFAULT_RETENTION_TIME,
                offsetData);

        log.trace("Sending offset-commit request with {} to coordinator {} for group {}", offsets, coordinator, groupId);
        // 发送OFFSET_COMMIT请求
        return client.send(coordinator, ApiKeys.OFFSET_COMMIT, req)
                .compose(new OffsetCommitResponseHandler(offsets));
    }

    public static class DefaultOffsetCommitCallback implements OffsetCommitCallback {
        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            if (exception != null)
                log.error("Offset commit failed.", exception);
        }
    }

    // 处理OFFSET_COMMIT请求的响应消息
    private class OffsetCommitResponseHandler extends CoordinatorResponseHandler<OffsetCommitResponse, Void> {

        private final Map<TopicPartition, OffsetAndMetadata> offsets;

        public OffsetCommitResponseHandler(Map<TopicPartition, OffsetAndMetadata> offsets) {
            this.offsets = offsets;
        }

        @Override
        public OffsetCommitResponse parse(ClientResponse response) {
            return new OffsetCommitResponse(response.responseBody());
        }

        @Override
        public void handle(OffsetCommitResponse commitResponse, RequestFuture<Void> future) {
            sensors.commitLatency.record(response.requestLatencyMs());
            Set<String> unauthorizedTopics = new HashSet<>();

            for (Map.Entry<TopicPartition, Short> entry : commitResponse.responseData().entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetAndMetadata offsetAndMetadata = this.offsets.get(tp);
                long offset = offsetAndMetadata.offset();

                Errors error = Errors.forCode(entry.getValue());
                if (error == Errors.NONE) {
                    log.debug("Group {} committed offset {} for partition {}", groupId, offset, tp);
                    if (subscriptions.isAssigned(tp))
                        // update the local cache only if the partition is still assigned
                        // 更新对应topic-partition的TopicPartitionState里的OffsetAndMetadata committed
                        subscriptions.committed(tp, offsetAndMetadata);
                } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                    log.error("Not authorized to commit offsets for group {}", groupId);
                    future.raise(new GroupAuthorizationException(groupId));
                    return;
                } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                    unauthorizedTopics.add(tp.topic());
                } else if (error == Errors.OFFSET_METADATA_TOO_LARGE
                        || error == Errors.INVALID_COMMIT_OFFSET_SIZE) {
                    // raise the error to the user
                    log.debug("Offset commit for group {} failed on partition {}: {}", groupId, tp, error.message());
                    future.raise(error);
                    return;
                } else if (error == Errors.GROUP_LOAD_IN_PROGRESS) {
                    // just retry
                    log.debug("Offset commit for group {} failed: {}", groupId, error.message());
                    future.raise(error);
                    return;
                } else if (error == Errors.GROUP_COORDINATOR_NOT_AVAILABLE
                        || error == Errors.NOT_COORDINATOR_FOR_GROUP
                        || error == Errors.REQUEST_TIMED_OUT) {
                    log.debug("Offset commit for group {} failed: {}", groupId, error.message());
                    coordinatorDead();
                    future.raise(error);
                    return;
                } else if (error == Errors.UNKNOWN_MEMBER_ID
                        || error == Errors.ILLEGAL_GENERATION
                        || error == Errors.REBALANCE_IN_PROGRESS) {
                    // need to re-join group
                    log.debug("Offset commit for group {} failed: {}", groupId, error.message());
                    subscriptions.needReassignment();
                    future.raise(new CommitFailedException("Commit cannot be completed since the group has already " +
                            "rebalanced and assigned the partitions to another member. This means that the time " +
                            "between subsequent calls to poll() was longer than the configured session.timeout.ms, " +
                            "which typically implies that the poll loop is spending too much time message processing. " +
                            "You can address this either by increasing the session timeout or by reducing the maximum " +
                            "size of batches returned in poll() with max.poll.records."));
                    return;
                } else {
                    log.error("Group {} failed to commit partition {} at offset {}: {}", groupId, tp, offset, error.message());
                    future.raise(new KafkaException("Unexpected error in commit: " + error.message()));
                    return;
                }
            }

            if (!unauthorizedTopics.isEmpty()) {
                log.error("Not authorized to commit to topics {} for group {}", unauthorizedTopics, groupId);
                future.raise(new TopicAuthorizationException(unauthorizedTopics));
            } else {
                future.complete(null);
            }
        }
    }

    /**
     * Fetch the committed offsets for a set of partitions. This is a non-blocking call. The
     * returned future can be polled to get the actual offsets returned from the broker.
     *
     * @param partitions The set of partitions to get offsets for.
     * @return A request future containing the committed offsets.
     */
    // 创建OFFSET_FETCH请求
    private RequestFuture<Map<TopicPartition, OffsetAndMetadata>> sendOffsetFetchRequest(Set<TopicPartition> partitions) {
        if (coordinatorUnknown())
            return RequestFuture.coordinatorNotAvailable();

        log.debug("Group {} fetching committed offsets for partitions: {}", groupId, partitions);
        // construct the request
        OffsetFetchRequest request = new OffsetFetchRequest(this.groupId, new ArrayList<>(partitions));

        // send the request with a callback
        return client.send(coordinator, ApiKeys.OFFSET_FETCH, request)
                .compose(new OffsetFetchResponseHandler());
    }
    // 处理OFFSET_FETCH请求的响应
    // 服务端响应的封装在代码：kafka.coordinator.GroupMetadataManager.getOffsets
    private class OffsetFetchResponseHandler extends CoordinatorResponseHandler<OffsetFetchResponse, Map<TopicPartition, OffsetAndMetadata>> {

        @Override
        public OffsetFetchResponse parse(ClientResponse response) {
            return new OffsetFetchResponse(response.responseBody());
        }

        @Override
        public void handle(OffsetFetchResponse response, RequestFuture<Map<TopicPartition, OffsetAndMetadata>> future) {
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(response.responseData().size());
            for (Map.Entry<TopicPartition, OffsetFetchResponse.PartitionData> entry : response.responseData().entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetFetchResponse.PartitionData data = entry.getValue();
                // 有异常
                if (data.hasError()) {
                    Errors error = Errors.forCode(data.errorCode);
                    log.debug("Group {} failed to fetch offset for partition {}: {}", groupId, tp, error.message());

                    if (error == Errors.GROUP_LOAD_IN_PROGRESS) {
                        // just retry
                        future.raise(error);
                    } else if (error == Errors.NOT_COORDINATOR_FOR_GROUP) {
                        // re-discover the coordinator and retry
                        coordinatorDead();
                        future.raise(error);
                    } else if (error == Errors.UNKNOWN_MEMBER_ID
                            || error == Errors.ILLEGAL_GENERATION) {
                        // need to re-join group
                        subscriptions.needReassignment();
                        future.raise(error);
                    } else {
                        future.raise(new KafkaException("Unexpected error in fetch offset response: " + error.message()));
                    }
                    return;
                // 无异常信息并有提交offset
                } else if (data.offset >= 0) {
                    // record the position with the offset (-1 indicates no committed offset to fetch)
                    // 这里只需要记录带有偏移量的topic-partition即可，如果是-1那就说明没有提交偏移量
                    offsets.put(tp, new OffsetAndMetadata(data.offset, data.metadata));
                } else {
                    log.debug("Group {} has no committed offset for partition {}", groupId, tp);
                }
            }
            // 执行回调
            future.complete(offsets);
        }
    }

    private class ConsumerCoordinatorMetrics {
        public final Metrics metrics;
        public final String metricGrpName;

        public final Sensor commitLatency;

        public ConsumerCoordinatorMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metrics = metrics;
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            this.commitLatency = metrics.sensor("commit-latency");
            this.commitLatency.add(metrics.metricName("commit-latency-avg",
                this.metricGrpName,
                "The average time taken for a commit request"), new Avg());
            this.commitLatency.add(metrics.metricName("commit-latency-max",
                this.metricGrpName,
                "The max time taken for a commit request"), new Max());
            this.commitLatency.add(metrics.metricName("commit-rate",
                this.metricGrpName,
                "The number of commit calls per second"), new Rate(new Count()));

            Measurable numParts =
                new Measurable() {
                    public double measure(MetricConfig config, long now) {
                        return subscriptions.assignedPartitions().size();
                    }
                };
            metrics.addMetric(metrics.metricName("assigned-partitions",
                this.metricGrpName,
                "The number of partitions currently assigned to this consumer"), numParts);
        }
    }

    private static class MetadataSnapshot {
        private final Map<String, Integer> partitionsPerTopic;

        public MetadataSnapshot(SubscriptionState subscription, Cluster cluster) {
            Map<String, Integer> partitionsPerTopic = new HashMap<>();
            for (String topic : subscription.groupSubscription())
                partitionsPerTopic.put(topic, cluster.partitionCountForTopic(topic));
            this.partitionsPerTopic = partitionsPerTopic;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MetadataSnapshot that = (MetadataSnapshot) o;
            return partitionsPerTopic != null ? partitionsPerTopic.equals(that.partitionsPerTopic) : that.partitionsPerTopic == null;
        }

        @Override
        public int hashCode() {
            return partitionsPerTopic != null ? partitionsPerTopic.hashCode() : 0;
        }
    }


}
