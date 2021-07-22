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
package org.apache.kafka.clients;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 元数据类
 * A class encapsulating some of the logic around metadata.
 * <p>
 * This class is shared by the client thread (for partitioning) and the background sender thread.
 * 这个类由客户线程(用于分区也就是kafka producer)和后台发送方线程共享。
 * 
 * Metadata is maintained for only a subset of topics, which can be added to over time. When we request metadata for a
 * topic we don't have any metadata for it will trigger a metadata update.
 * metadata作为所有主题的一个子集，当我们请求原数据时，也就是发送数据，如果没有原数据，它会触发元数据更新操作
 */
public final class Metadata {

    private static final Logger log = LoggerFactory.getLogger(Metadata.class);
    // 发送消息失败后的重试间隔，默认100ms
    private final long refreshBackoffMs;
    // metadata自动刷新的时间间隔，默认 5 * 60 * 1000ms
    private final long metadataExpireMs;
    // 版本号,每次更新后+1,初始化为0
    private int version;
    // 上一次刷新metadata的时间戳,初始化为0
    private long lastRefreshMs;
    // 上一次成功刷新metadata的时间戳,初始化为0
    private long lastSuccessfulRefreshMs;
    // 集群信息,初始化为Cluster.empty();
    private Cluster cluster;
    // 是否需要更新标识,初始化为false
    private boolean needUpdate;
    // 记录包含的主题,初始化为new HashSet<String>()
    private final Set<String> topics;
    // 监听者,初始化为new ArrayList<>()
    private final List<Listener> listeners;
    // 是否需要拉取所有的topic,初始化为false
    private boolean needMetadataForAllTopics;

    /**
     * Create a metadata instance with reasonable defaults
     */
    public Metadata() {
        this(100L, 60 * 60 * 1000L);
    }

    /**
     * Create a new Metadata instance
     * @param refreshBackoffMs The minimum amount of time that must expire between metadata refreshes to avoid busy
     *        polling
     * @param metadataExpireMs The maximum amount of time that metadata can be retained without refresh
     */
    public Metadata(long refreshBackoffMs, long metadataExpireMs) {
        this.refreshBackoffMs = refreshBackoffMs;// 默认100ms
        this.metadataExpireMs = metadataExpireMs;// 默认5 * 60 * 1000ms
        this.lastRefreshMs = 0L;
        this.lastSuccessfulRefreshMs = 0L;
        this.version = 0;
        this.cluster = Cluster.empty();
        this.needUpdate = false;
        this.topics = new HashSet<String>();
        this.listeners = new ArrayList<>();
        this.needMetadataForAllTopics = false;
    }

    /**
     * Get the current cluster info without blocking
     */
    public synchronized Cluster fetch() {
        return this.cluster;
    }

    /**
     * Add the topic to maintain in the metadata
     */
    public synchronized void add(String topic) {
        topics.add(topic);
    }

    /**
     * The next time to update the cluster info is the maximum of the time the current info will expire and the time the
     * current info can be updated (i.e. backoff time has elapsed); If an update has been request then the expiry time
     * is now
     * 计算下次更新metadata的时间戳
     */
    public synchronized long timeToNextUpdate(long nowMs) {
        long timeToExpire = needUpdate ? 0 : Math.max(this.lastSuccessfulRefreshMs + this.metadataExpireMs - nowMs, 0);
        long timeToAllowUpdate = this.lastRefreshMs + this.refreshBackoffMs - nowMs;
        return Math.max(timeToExpire, timeToAllowUpdate);
    }

    /**
     * Request an update of the current cluster metadata info, return the current version before the update
     * 更新metadata时调用该接口，返回metadata当前版本号，或标注metadata待更新
     */
    public synchronized int requestUpdate() {
        this.needUpdate = true;
        return this.version;
    }

    /**
     * Check whether an update has been explicitly requested.
     * @return true if an update was requested, false otherwise
     * 是否需要更新metadata
     */
    public synchronized boolean updateRequested() {
        return this.needUpdate;
    }

    /**
     * Wait for metadata update until the current version is larger than the last version we know of
     * 等待metadata更新，直到当前版本大于我们知道的上一个版本
     */
    public synchronized void awaitUpdate(final int lastVersion, final long maxWaitMs) throws InterruptedException {
        if (maxWaitMs < 0) {
            throw new IllegalArgumentException("Max time to wait for metadata updates should not be < 0 milli seconds");
        }
        long begin = System.currentTimeMillis();
        long remainingWaitMs = maxWaitMs;
        // 获取当前版本与上一次的版本，如果未更新则执行等待更新
        while (this.version <= lastVersion) {
            if (remainingWaitMs != 0)
                wait(remainingWaitMs);
            // 计算更新耗时
            long elapsed = System.currentTimeMillis() - begin;
            // 超时抛出异常
            if (elapsed >= maxWaitMs)
                throw new TimeoutException("Failed to update metadata after " + maxWaitMs + " ms.");
            // 计算剩余时间
            remainingWaitMs = maxWaitMs - elapsed;
        }
    }

    /**
     * Replace the current set of topics maintained to the one provided
     * @param topics
     * 将当前维护的主题集替换为提供的主题集合
     */
    public synchronized void setTopics(Collection<String> topics) {
        // 如果当前集合不完全包含替换集合中的topic，那么将metadata标注为待更新
        if (!this.topics.containsAll(topics))
            requestUpdate();
        this.topics.clear();
        this.topics.addAll(topics);
    }

    /**
     * Get the list of topics we are currently maintaining metadata for
     */
    public synchronized Set<String> topics() {
        return new HashSet<String>(this.topics);
    }

    /**
     * Check if a topic is already in the topic set.
     * @param topic topic to check
     * @return true if the topic exists, false otherwise
     */
    public synchronized boolean containsTopic(String topic) {
        return this.topics.contains(topic);
    }

    /**
     * Update the cluster metadata
     * 更新metadata 该方法调用地点如下:
     * KafkaProducer初始化329行: {@link org.apache.kafka.clients.producer.KafkaProducer#KafkaProducer}
     * KafkaConsumer初始化642行： {@link KafkaConsumer#KafkaConsumer}
     */
    public synchronized void update(Cluster cluster, long now) {
        // 修改needUpdate为false,表示不需要刷新了
        this.needUpdate = false;
        // 更新最近一次刷新以及成功刷新metadata的时间戳为当前时间
        this.lastRefreshMs = now;
        this.lastSuccessfulRefreshMs = now;
        // 版本号+1
        this.version += 1;
        // 遍历listeners,初始化时为空
        for (Listener listener: listeners)
            listener.onMetadataUpdate(cluster);

        // Do this after notifying listeners as subscribed topics' list can be changed by listeners
        // needMetadataForAllTopics默认初始化为false,在调用地点1的情况下,返回cluster,也就是"bootstrap.servers"配置的服务列表List<InetSocketAddress>
        this.cluster = this.needMetadataForAllTopics ? getClusterForCurrentTopics(cluster) : cluster;
        // 唤醒,因为更新上下文,此时可能KafkaProducer阻塞在
        // org.apache.kafka.clients.producer.KafkaProducer.doSend -->
        // org.apache.kafka.clients.producer.KafkaProducer.waitOnMetadata -->
        // org.apache.kafka.clients.Metadata.awaitUpdate方法上
        // 所以此时唤醒它,继续往下执行发送消息
        notifyAll();
        log.debug("Updated cluster metadata version {} to {}", this.version, this.cluster);
    }

    /**
     * Record an attempt to update the metadata that failed. We need to keep track of this
     * to avoid retrying immediately.
     */
    public synchronized void failedUpdate(long now) {
        this.lastRefreshMs = now;
    }
    
    /**
     * @return The current metadata version
     */
    public synchronized int version() {
        return this.version;
    }

    /**
     * The last time metadata was successfully updated.
     */
    public synchronized long lastSuccessfulUpdate() {
        return this.lastSuccessfulRefreshMs;
    }

    /**
     * The metadata refresh backoff in ms
     */
    public long refreshBackoff() {
        return refreshBackoffMs;
    }

    /**
     * Set state to indicate if metadata for all topics in Kafka cluster is required or not.
     * @param needMetadataForAllTopics boolean indicating need for metadata of all topics in cluster.
     * 设置state来指示是否需要Kafka集群中所有主题的元数据
     */
    public synchronized void needMetadataForAllTopics(boolean needMetadataForAllTopics) {
        this.needMetadataForAllTopics = needMetadataForAllTopics;
    }

    /**
     * Get whether metadata for all topics is needed or not
     */
    public synchronized boolean needMetadataForAllTopics() {
        return this.needMetadataForAllTopics;
    }

    /**
     * Add a Metadata listener that gets notified of metadata updates
     */
    public synchronized void addListener(Listener listener) {
        this.listeners.add(listener);
    }

    /**
     * Stop notifying the listener of metadata updates
     */
    public synchronized void removeListener(Listener listener) {
        this.listeners.remove(listener);
    }

    /**
     * MetadataUpdate Listener
     */
    public interface Listener {
        void onMetadataUpdate(Cluster cluster);
    }

    private Cluster getClusterForCurrentTopics(Cluster cluster) {
        Set<String> unauthorizedTopics = new HashSet<>();
        Collection<PartitionInfo> partitionInfos = new ArrayList<>();
        List<Node> nodes = Collections.emptyList();
        if (cluster != null) {
            unauthorizedTopics.addAll(cluster.unauthorizedTopics());
            unauthorizedTopics.retainAll(this.topics);

            for (String topic : this.topics) {
                partitionInfos.addAll(cluster.partitionsForTopic(topic));
            }
            nodes = cluster.nodes();
        }
        return new Cluster(nodes, partitionInfos, unauthorizedTopics);
    }
}
