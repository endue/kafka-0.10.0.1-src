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

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A class for tracking the topics, partitions, and offsets for the consumer.
 * 一个跟踪记录消费者的主题、分区以及偏移量的类
 *
 * A partition is "assigned" either directly with {@link #assignFromUser(Collection)} (manual assignment)
 * or with {@link #assignFromSubscribed(Collection)} (automatic assignment from subscription).
 * 一个分区可以通过“assignFromUser(Collection)”的方式手动分配，也可用通过assignFromSubscribed(Collection)的方式自动分配
 *
 * Once assigned, the partition is not considered "fetchable" until its initial position has
 * been set with {@link #seek(TopicPartition, long)}.
 * 分区被分配后，在调用“seek(TopicPartition, long)”方法初始化它的拉取位置之前，会一直被认为是不可读的
 *
 * Fetchable partitions track a fetch position which is used to set the offset of the next fetch,
 * and a consumed position which is the last offset that has been returned to the user.
 * 可读的分区，会有一个读取位置，该位置用于记录下一个读取位置的偏移量，
 * 还有一个被消耗的位置，用来记录当前consumer消耗的位置，也就是返回给用户的最后一个偏移量
 *
 * You can suspend fetching from a partition through {@link #pause(TopicPartition)} without
 * affecting the fetched/consumed offsets.
 * 可以通过“pause(TopicPartition)”方法暂停从某个分区的抓取，这样不会影响所获取的、消耗的偏移量
 *
 * The partition will remain unfetchable until the {@link #resume(TopicPartition)} is used.
 * 该分区会一直暂停从某个分区的抓取，直到调用“resume(TopicPartition)”方法
 *
 * You can also query the pause state independently with {@link #isPaused(TopicPartition)}.
 * 也可以查看某个分区抓取的暂停状态是否为true或false,通过“isPaused(TopicPartition)”
 *
 * Note that pause state as well as fetch/consumed positions are not preserved when partition
 * assignment is changed whether directly by the user or through a group rebalance.
 *
 * This class also maintains a cache of the latest commit position for each of the assigned
 * partitions. This is updated through {@link #committed(TopicPartition, OffsetAndMetadata)} and can be used
 * to set the initial fetch position (e.g. {@link Fetcher#resetOffset(TopicPartition)}.
 */
public class SubscriptionState {

    // 定义类型
    private enum SubscriptionType {
        NONE, AUTO_TOPICS, AUTO_PATTERN, USER_ASSIGNED
    };

    /* the type of subscription */
    // 记录订阅类型
    private SubscriptionType subscriptionType;

    /* the pattern user has requested */
    // 订阅模式，表示定义匹配的所有主题
    private Pattern subscribedPattern;

    /* the list of topics the user has requested */
    // 记录用户订阅的topic
    private final Set<String> subscription;

    /* the list of topics the group has subscribed to (set only for the leader on join group completion) */
    // 记录组成员订阅的所有topic
    private final Set<String> groupSubscription;

    /* the list of partitions the user has requested */
    // 记录用户指定分配的topic-partition
    private final Set<TopicPartition> userAssignment;

    /* the list of partitions currently assigned */
    // 记录当前分配的topic-partition以及对应的状态
    private final Map<TopicPartition, TopicPartitionState> assignment;

    /* do we need to request a partition assignment from the coordinator? */
    // 记录当前consumer是否需要被分配分区
    // 当用户手动指定topic-partition时，该值为false
    private boolean needsPartitionAssignment;

    /* do we need to request the latest committed offsets from the coordinator? */
    // 记录是否需要提交offset
    private boolean needsFetchCommittedOffsets;

    /* Default offset reset strategy */
    // 偏移量重置策略
    private final OffsetResetStrategy defaultResetStrategy;

    /* Listener to be invoked when assignment changes */
    // 分区Rebalance后的回调监听
    private ConsumerRebalanceListener listener;

    private static final String SUBSCRIPTION_EXCEPTION_MESSAGE =
        "Subscription to topics, partitions and pattern are mutually exclusive";

    /**
     * This method sets the subscription type if it is not already set (i.e. when it is NONE),
     * or verifies that the subscription type is equal to the give type when it is set (i.e.
     * when it is not NONE)
     * @param type The given subscription type
     */
    private void setSubscriptionType(SubscriptionType type) {
        if (this.subscriptionType == SubscriptionType.NONE)
            this.subscriptionType = type;
        else if (this.subscriptionType != type)
            throw new IllegalStateException(SUBSCRIPTION_EXCEPTION_MESSAGE);
    }

    public SubscriptionState(OffsetResetStrategy defaultResetStrategy) {
        this.defaultResetStrategy = defaultResetStrategy;
        this.subscription = new HashSet<>();
        this.userAssignment = new HashSet<>();
        this.assignment = new HashMap<>();
        this.groupSubscription = new HashSet<>();
        this.needsPartitionAssignment = false;
        this.needsFetchCommittedOffsets = true; // initialize to true for the consumers to fetch offset upon starting up
        this.subscribedPattern = null;
        this.subscriptionType = SubscriptionType.NONE;
    }

    /**
     * 订阅主题
     * @param topics 主题集合
     * @param listener 重分配时的回调
     */
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        // 这里订阅主题一定要有listener
        if (listener == null)
            throw new IllegalArgumentException("RebalanceListener cannot be null");
        // 设置订阅类型
        setSubscriptionType(SubscriptionType.AUTO_TOPICS);

        this.listener = listener;

        changeSubscription(topics);
    }

    /**
     * 修改订阅的topic
     * @param topicsToSubscribe
     */
    public void changeSubscription(Collection<String> topicsToSubscribe) {
        if (!this.subscription.equals(new HashSet<>(topicsToSubscribe))) {
            // 清空用户订阅的所有主题
            this.subscription.clear();
            // 添加新主题到用户订阅的主题和消费者组订阅的主题
            this.subscription.addAll(topicsToSubscribe);
            this.groupSubscription.addAll(topicsToSubscribe);
            this.needsPartitionAssignment = true;

            // Remove any assigned partitions which are no longer subscribed to
            // 删除不再订阅topic的那些已经被分配的分区
            for (Iterator<TopicPartition> it = assignment.keySet().iterator(); it.hasNext(); ) {
                TopicPartition tp = it.next();
                if (!subscription.contains(tp.topic()))
                    it.remove();
            }
        }
    }

    /**
     * Add topics to the current group subscription. This is used by the group leader to ensure
     * that it receives metadata updates for all topics that the group is interested in.
     * @param topics The topics to add to the group subscription
     *  添加消费者的主题，确保group leader能接收到所在组感兴趣的所有的topic的元数据
     */
    public void groupSubscribe(Collection<String> topics) {
        if (this.subscriptionType == SubscriptionType.USER_ASSIGNED)
            throw new IllegalStateException(SUBSCRIPTION_EXCEPTION_MESSAGE);
        this.groupSubscription.addAll(topics);
    }

    // 需要重新一次assign
    public void needReassignment() {
        this.groupSubscription.retainAll(subscription);
        this.needsPartitionAssignment = true;
    }

    /**
     * Change the assignment to the specified partitions provided by the user,
     * note this is different from {@link #assignFromSubscribed(Collection)}
     * whose input partitions are provided from the subscribed topics.
     */
    // 手动订阅topic-partition
    public void assignFromUser(Collection<TopicPartition> partitions) {
        // 设置订阅类型
        setSubscriptionType(SubscriptionType.USER_ASSIGNED);

        this.userAssignment.clear();
        this.userAssignment.addAll(partitions);

        for (TopicPartition partition : partitions)
            if (!assignment.containsKey(partition))
                addAssignedPartition(partition);

        this.assignment.keySet().retainAll(this.userAssignment);
        // 设置不需要被指定分区
        this.needsPartitionAssignment = false;
        this.needsFetchCommittedOffsets = true;
    }

    /**
     * Change the assignment to the specified partitions returned from the coordinator,
     * note this is different from {@link #assignFromUser(Collection)} which directly set the assignment from user inputs
     */
    public void assignFromSubscribed(Collection<TopicPartition> assignments) {
        for (TopicPartition tp : assignments)
            if (!this.subscription.contains(tp.topic()))
                throw new IllegalArgumentException("Assigned partition " + tp + " for non-subscribed topic.");
        // 清空旧的分配结果
        this.assignment.clear();
        // 记录新的分配结果
        for (TopicPartition tp: assignments)
            addAssignedPartition(tp);
        this.needsPartitionAssignment = false;
    }

    public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        if (listener == null)
            throw new IllegalArgumentException("RebalanceListener cannot be null");

        setSubscriptionType(SubscriptionType.AUTO_PATTERN);

        this.listener = listener;
        this.subscribedPattern = pattern;
    }

    public boolean hasPatternSubscription() {
        return this.subscriptionType == SubscriptionType.AUTO_PATTERN;
    }

    // 取消订阅
    public void unsubscribe() {
        this.subscription.clear();
        this.userAssignment.clear();
        this.assignment.clear();
        this.needsPartitionAssignment = true;
        this.subscribedPattern = null;
        this.subscriptionType = SubscriptionType.NONE;
    }


    public Pattern getSubscribedPattern() {
        return this.subscribedPattern;
    }

    public Set<String> subscription() {
        return this.subscription;
    }

    public Set<TopicPartition> pausedPartitions() {
        HashSet<TopicPartition> paused = new HashSet<>();
        for (Map.Entry<TopicPartition, TopicPartitionState> entry : assignment.entrySet()) {
            final TopicPartition tp = entry.getKey();
            final TopicPartitionState state = entry.getValue();
            if (state.paused) {
                paused.add(tp);
            }
        }
        return paused;
    }

    /**
     * Get the subscription for the group. For the leader, this will include the union of the
     * subscriptions of all group members. For followers, it is just that member's subscription.
     * This is used when querying topic metadata to detect the metadata changes which would
     * require rebalancing. The leader fetches metadata for all topics in the group so that it
     * can do the partition assignment (which requires at least partition counts for all topics
     * to be assigned).
     * @return The union of all subscribed topics in the group if this member is the leader
     *   of the current generation; otherwise it returns the same set as {@link #subscription()}
     */
    public Set<String> groupSubscription() {
        return this.groupSubscription;
    }

    private TopicPartitionState assignedState(TopicPartition tp) {
        TopicPartitionState state = this.assignment.get(tp);
        if (state == null)
            throw new IllegalStateException("No current assignment for partition " + tp);
        return state;
    }

    public void committed(TopicPartition tp, OffsetAndMetadata offset) {
        assignedState(tp).committed(offset);
    }

    public OffsetAndMetadata committed(TopicPartition tp) {
        return assignedState(tp).committed;
    }

    public void needRefreshCommits() {
        this.needsFetchCommittedOffsets = true;
    }

    public boolean refreshCommitsNeeded() {
        return this.needsFetchCommittedOffsets;
    }

    public void commitsRefreshed() {
        this.needsFetchCommittedOffsets = false;
    }

    // 设置指定topic-partition的消费offset
    public void seek(TopicPartition tp, long offset) {
        assignedState(tp).seek(offset);
    }

    public Set<TopicPartition> assignedPartitions() {
        return this.assignment.keySet();
    }

    public Set<TopicPartition> fetchablePartitions() {
        Set<TopicPartition> fetchable = new HashSet<>();
        for (Map.Entry<TopicPartition, TopicPartitionState> entry : assignment.entrySet()) {
            if (entry.getValue().isFetchable())
                fetchable.add(entry.getKey());
        }
        return fetchable;
    }

    /**
     * 判断是否为自动分配分区
     * @return
     */
    public boolean partitionsAutoAssigned() {
        return this.subscriptionType == SubscriptionType.AUTO_TOPICS || this.subscriptionType == SubscriptionType.AUTO_PATTERN;
    }

    public void position(TopicPartition tp, long offset) {
        assignedState(tp).position(offset);
    }

    public Long position(TopicPartition tp) {
        return assignedState(tp).position;
    }

    // 获取所有topic-partition下一次拉取消息的offset
    public Map<TopicPartition, OffsetAndMetadata> allConsumed() {
        // 要返回的结果
        Map<TopicPartition, OffsetAndMetadata> allConsumed = new HashMap<>();
        // 遍历当前consumer负责的所有topic-partition
        for (Map.Entry<TopicPartition, TopicPartitionState> entry : assignment.entrySet()) {
            // 获取对应TopicPartitionState里的position准备进行提交
            TopicPartitionState state = entry.getValue();
            if (state.hasValidPosition())
                allConsumed.put(entry.getKey(), new OffsetAndMetadata(state.position));
        }
        return allConsumed;
    }

    // 根据offsetResetStrategy设置对应topic-partition的offset
    public void needOffsetReset(TopicPartition partition, OffsetResetStrategy offsetResetStrategy) {
        assignedState(partition).awaitReset(offsetResetStrategy);
    }

    public void needOffsetReset(TopicPartition partition) {
        needOffsetReset(partition, defaultResetStrategy);
    }

    public boolean hasDefaultOffsetResetPolicy() {
        return defaultResetStrategy != OffsetResetStrategy.NONE;
    }

    public boolean isOffsetResetNeeded(TopicPartition partition) {
        return assignedState(partition).awaitingReset();
    }

    public OffsetResetStrategy resetStrategy(TopicPartition partition) {
        return assignedState(partition).resetStrategy;
    }

    public boolean hasAllFetchPositions() {
        for (TopicPartitionState state : assignment.values())
            if (!state.hasValidPosition())
                return false;
        return true;
    }

    /**
     * 过滤出topic-partition对应的TopicPartitionState对象的position == null
     * 的topic-partition，这些分区是不知道应该从哪里拉取消息，所以需要从GroupCoordinator
     * 中拉取
     * @return
     */
    public Set<TopicPartition> missingFetchPositions() {
        Set<TopicPartition> missing = new HashSet<>();
        // 遍历被分配的topic-partition和其对应的TopicPartitionState实例
        for (Map.Entry<TopicPartition, TopicPartitionState> entry : assignment.entrySet())
            // 如果TopicPartitionState实例的position == null
            // 那么记录当前topic-partition
            if (!entry.getValue().hasValidPosition())
                missing.add(entry.getKey());
        return missing;
    }

    public boolean partitionAssignmentNeeded() {
        return this.needsPartitionAssignment;
    }

    public boolean isAssigned(TopicPartition tp) {
        return assignment.containsKey(tp);
    }

    public boolean isPaused(TopicPartition tp) {
        return isAssigned(tp) && assignedState(tp).paused;
    }

    public boolean isFetchable(TopicPartition tp) {
        return isAssigned(tp) && assignedState(tp).isFetchable();
    }

    public void pause(TopicPartition tp) {
        assignedState(tp).pause();
    }

    public void resume(TopicPartition tp) {
        assignedState(tp).resume();
    }

    private void addAssignedPartition(TopicPartition tp) {
        this.assignment.put(tp, new TopicPartitionState());
    }

    public ConsumerRebalanceListener listener() {
        return listener;
    }

    // 分区状态
    private static class TopicPartitionState {
        // 对应topic-partition下一次需要拉取消息的偏移量
        private Long position; // last consumed position
        // 对应topic-partition已经提交的偏移量
        private OffsetAndMetadata committed;  // last committed position
        // 分区是否被暂停
        private boolean paused;  // whether this partition has been paused by the user
        // 重置策略
        private OffsetResetStrategy resetStrategy;  // the strategy to use if the offset needs resetting

        public TopicPartitionState() {
            this.paused = false;
            this.position = null;
            this.committed = null;
            this.resetStrategy = null;
        }
        // 基于OffsetResetStrategy的重置offset
        private void awaitReset(OffsetResetStrategy strategy) {
            this.resetStrategy = strategy;
            this.position = null;
        }

        public boolean awaitingReset() {
            return resetStrategy != null;
        }

        public boolean hasValidPosition() {
            return position != null;
        }

        private void seek(long offset) {
            this.position = offset;
            this.resetStrategy = null;
        }

        private void position(long offset) {
            if (!hasValidPosition())
                throw new IllegalStateException("Cannot set a new position without a valid current position");
            this.position = offset;
        }

        private void committed(OffsetAndMetadata offset) {
            this.committed = offset;
        }

        private void pause() {
            this.paused = true;
        }

        private void resume() {
            this.paused = false;
        }

        private boolean isFetchable() {
            return !paused && hasValidPosition();
        }

    }

}
