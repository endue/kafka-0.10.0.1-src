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
package org.apache.kafka.clients.producer.internals;

import java.util.Iterator;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.utils.CopyOnWriteMap;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class acts as a queue that accumulates records into {@link org.apache.kafka.common.record.MemoryRecords}
 * instances to be sent to the server.
 * <p>
 * The accumulator uses a bounded amount of memory and append calls will block when that memory is exhausted, unless
 * this behavior is explicitly disabled.
 */
public final class RecordAccumulator {

    private static final Logger log = LoggerFactory.getLogger(RecordAccumulator.class);
    // 记录RecordAccumulator状态是否关闭
    private volatile boolean closed;
    // 记录flush操作的线程数
    private final AtomicInteger flushesInProgress;
    // 记录写消息的线程数
    private final AtomicInteger appendsInProgress;
    // 记录每次申请RecordBatch的大小，默认16kb
    private final int batchSize;
    // 压缩类型,默认none
    private final CompressionType compression;
    // RecordBatch等待lingerMs后，必须发送出去
    private final long lingerMs;
    // RecordBatch重试需要等待的时间(默认100ms)
    private final long retryBackoffMs;
    // BufferPool，一个ByteBuffer池子
    private final BufferPool free;
    private final Time time;
    // 记录TopicPartition与自身消息集合Deque<RecordBatch>
    private final ConcurrentMap<TopicPartition, Deque<RecordBatch>> batches;
    // 记录待发送的RecordBatch
    private final IncompleteRecordBatches incomplete;
    // The following variables are only accessed by the sender thread, so we don't need to protect them.
    // 记录所有需要保证消息有序性的分区topic-partition
    private final Set<TopicPartition> muted;
    private int drainIndex;

    /**
     * Create a new record accumulator
     * 
     * @param batchSize The size to use when allocating {@link org.apache.kafka.common.record.MemoryRecords} instances
     * @param totalSize The maximum memory the record accumulator can use.
     * @param compression The compression codec for the records
     * @param lingerMs An artificial delay time to add before declaring a records instance that isn't full ready for
     *        sending. This allows time for more records to arrive. Setting a non-zero lingerMs will trade off some
     *        latency for potentially better throughput due to more batching (and hence fewer, larger requests).
     * @param retryBackoffMs An artificial delay time to retry the produce request upon receiving an error. This avoids
     *        exhausting all retries in a short period of time.
     * @param metrics The metrics
     * @param time The time instance to use
     */
    public RecordAccumulator(int batchSize,// 默认16384 = 16kb
                             long totalSize,// 默认32M
                             CompressionType compression,// 默认none
                             long lingerMs,// 默认0
                             long retryBackoffMs,// 默认100ms
                             Metrics metrics,
                             Time time) {// 默认当前时间
        this.drainIndex = 0;
        this.closed = false;
        this.flushesInProgress = new AtomicInteger(0);
        this.appendsInProgress = new AtomicInteger(0);
        this.batchSize = batchSize;
        this.compression = compression;
        this.lingerMs = lingerMs;
        this.retryBackoffMs = retryBackoffMs;
        this.batches = new CopyOnWriteMap<>();
        String metricGrpName = "producer-metrics";
        this.free = new BufferPool(totalSize, batchSize, metrics, time, metricGrpName);
        this.incomplete = new IncompleteRecordBatches();
        this.muted = new HashSet<>();
        this.time = time;
        registerMetrics(metrics, metricGrpName);
    }

    private void registerMetrics(Metrics metrics, String metricGrpName) {
        MetricName metricName = metrics.metricName("waiting-threads", metricGrpName, "The number of user threads blocked waiting for buffer memory to enqueue their records");
        Measurable waitingThreads = new Measurable() {
            public double measure(MetricConfig config, long now) {
                return free.queued();
            }
        };
        metrics.addMetric(metricName, waitingThreads);

        metricName = metrics.metricName("buffer-total-bytes", metricGrpName, "The maximum amount of buffer memory the client can use (whether or not it is currently used).");
        Measurable totalBytes = new Measurable() {
            public double measure(MetricConfig config, long now) {
                return free.totalMemory();
            }
        };
        metrics.addMetric(metricName, totalBytes);

        metricName = metrics.metricName("buffer-available-bytes", metricGrpName, "The total amount of buffer memory that is not being used (either unallocated or in the free list).");
        Measurable availableBytes = new Measurable() {
            public double measure(MetricConfig config, long now) {
                return free.availableMemory();
            }
        };
        metrics.addMetric(metricName, availableBytes);

        Sensor bufferExhaustedRecordSensor = metrics.sensor("buffer-exhausted-records");
        metricName = metrics.metricName("buffer-exhausted-rate", metricGrpName, "The average per-second number of record sends that are dropped due to buffer exhaustion");
        bufferExhaustedRecordSensor.add(metricName, new Rate());
    }

    /**
     * Add a record to the accumulator, return the append result
     * <p>
     * The append result will contain the future metadata, and flag for whether the appended batch is full or a new batch is created
     * <p>
     *
     * @param tp The topic/partition to which this record is being sent record中记录的要发往的topic-partition
     * @param timestamp The timestamp of the record  record中记录的时间戳，用户没指定则使用doSend()方法调用时的时间戳
     * @param key The key for the record    record中记录的key
     * @param value The value for the record    record中记录的value
     * @param callback The user-supplied callback to execute when the request is complete 用户调用send()方法时，自定义的回调方法
     * @param maxTimeToBlock The maximum time in milliseconds to block for buffer memory to be available
     */
    public RecordAppendResult append(TopicPartition tp,// 消息发送的topic-partition
                                     long timestamp,// 消息的时间戳
                                     byte[] key,
                                     byte[] value,
                                     Callback callback,// 用户回调
                                     long maxTimeToBlock) throws InterruptedException {// 最大阻塞等待时间ms
        // We keep track of the number of appending thread to make sure we do not miss batches in
        // abortIncompleteBatches().
        // AtomicInteger类型累计并发线程数
        appendsInProgress.incrementAndGet();
        try {
            // check if we have an in-progress batch
            // 获取topic-partition对应的集合Deque
            Deque<RecordBatch> dq = getOrCreateDeque(tp);
            synchronized (dq) {
                if (closed)
                    throw new IllegalStateException("Cannot send after the producer is closed.");
                // 尝试将消息放到topic对应的Deque尾部的RecordBatch中
                RecordAppendResult appendResult = tryAppend(timestamp, key, value, callback, dq);
                if (appendResult != null)
                    return appendResult;
            }

            // we don't have an in-progress record batch try to allocate a new batch
            // 如果上一步失败，说明没有可用的RecordBatch，需要创建一个
            // 获取size大小：取 batchSize(默认16KB)和消息 中最大值
            int size = Math.max(this.batchSize, Records.LOG_OVERHEAD + Record.recordSize(key, value));
            log.trace("Allocating a new {} byte message buffer for topic {} partition {}", size, tp.topic(), tp.partition());
            // 基于size在BufferPool缓存池中申请一块buffer空间
            ByteBuffer buffer = free.allocate(size, maxTimeToBlock);
            synchronized (dq) {
                // Need to check if producer is closed again after grabbing the dequeue lock.
                if (closed)
                    throw new IllegalStateException("Cannot send after the producer is closed.");
                // 再次尝试将消息放到Deque尾部的RecordBatch中，以防多线程时已被其他线程创建
                RecordAppendResult appendResult = tryAppend(timestamp, key, value, callback, dq);
                // 消息放置成功说明RecordBatch已被其他线程创建，之前申请的ByteBuffer需要释放
                if (appendResult != null) {
                    // Somebody else found us a batch, return the one we waited for! Hopefully this doesn't happen often...
                    // 释放ByteBuffer
                    free.deallocate(buffer);
                    return appendResult;
                }
                // 走到这一步说明当前线程已申请了一块ByteBuffer空间并且没有其他线程在相同的topic中创建RecordBatch
                // 创建RecordBatch
                MemoryRecords records = MemoryRecords.emptyRecords(buffer, compression, this.batchSize);
                RecordBatch batch = new RecordBatch(tp, records, time.milliseconds());
                FutureRecordMetadata future = Utils.notNull(batch.tryAppend(timestamp, key, value, callback, time.milliseconds()));
                // 将RecordBatch保存到Deque的末尾
                dq.addLast(batch);
                // 将待发送RecordBatch记录到IncompleteRecordBatches队列中
                incomplete.add(batch);
                return new RecordAppendResult(future, dq.size() > 1 || batch.records.isFull(), true);
            }
        } finally {
            appendsInProgress.decrementAndGet();
        }
    }

    /**
     * If `RecordBatch.tryAppend` fails (i.e. the record batch is full), close its memory records to release temporary
     * resources (like compression streams buffers).
     *
     */
    // 将消息放到对应topic-partition的Deque<RecordBatch>尾部的RecordBatch中
    private RecordAppendResult tryAppend(long timestamp, byte[] key, byte[] value, Callback callback, Deque<RecordBatch> deque) {
        // 获取topic对应的Deque末尾的RecordBatch
        RecordBatch last = deque.peekLast();
        // 如果RecordBatch不为null继续执行，否则返回null
        if (last != null) {
            // 将消息放到RecordBatch中
            FutureRecordMetadata future = last.tryAppend(timestamp, key, value, callback, time.milliseconds());
            // 表示没有足够的空间，将当前MemoryRecords标记为不可写并调用底层buffer.flip()方法
            if (future == null)
                last.records.close();
            else
                return new RecordAppendResult(future, deque.size() > 1 || last.records.isFull(), false);
        }
        return null;
    }

    /**
     * Abort the batches that have been sitting in RecordAccumulator for more than the configured requestTimeout
     * due to metadata being unavailable
     * 丢弃过期的RecordBatch
     */
    public List<RecordBatch> abortExpiredBatches(int requestTimeout, long now) {
        List<RecordBatch> expiredBatches = new ArrayList<>();
        int count = 0;
        for (Map.Entry<TopicPartition, Deque<RecordBatch>> entry : this.batches.entrySet()) {
            Deque<RecordBatch> dq = entry.getValue();
            TopicPartition tp = entry.getKey();
            // We only check if the batch should be expired if the partition does not have a batch in flight.
            // This is to prevent later batches from being expired while an earlier batch is still in progress.
            // Note that `muted` is only ever populated if `max.in.flight.request.per.connection=1` so this protection
            // is only active in this case. Otherwise the expiration order is not guaranteed.
            if (!muted.contains(tp)) {
                synchronized (dq) {
                    // iterate over the batches and expire them if they have been in the accumulator for more than requestTimeOut
                    // 获取Deque末尾RecordBatch，添加的时候用的是dq.addLast();
                    // 也就是最新的一个RecordBatch
                    RecordBatch lastBatch = dq.peekLast();
                    Iterator<RecordBatch> batchIterator = dq.iterator();
                    while (batchIterator.hasNext()) {
                        RecordBatch batch = batchIterator.next();
                        boolean isFull = batch != lastBatch || batch.records.isFull();
                        // check if the batch is expired
                        // 判断是否过期
                        if (batch.maybeExpire(requestTimeout, retryBackoffMs, now, this.lingerMs, isFull)) {
                            expiredBatches.add(batch);
                            count++;
                            batchIterator.remove();
                            deallocate(batch);
                        } else {
                            // Stop at the first batch that has not expired.
                            break;
                        }
                    }
                }
            }
        }
        if (!expiredBatches.isEmpty())
            log.trace("Expired {} batches in accumulator", count);

        return expiredBatches;
    }

    /**
     * Re-enqueue the given record batch in the accumulator to retry
     * 消息重新进入RecordBatch并且还是放到了队头
     */
    public void reenqueue(RecordBatch batch, long now) {
        batch.attempts++;
        batch.lastAttemptMs = now;
        batch.lastAppendTime = now;
        batch.setRetry();
        Deque<RecordBatch> deque = getOrCreateDeque(batch.topicPartition);
        synchronized (deque) {
            deque.addFirst(batch);
        }
    }

    /**
     * Get a list of nodes whose partitions are ready to be sent, and the earliest time at which any non-sendable
     * partition will be ready; Also return the flag for whether there are any unknown leaders for the accumulated
     * partition batches.
     * <p>
     * A destination node is ready to send data if:
     * <ol>
     * <li>There is at least one partition that is not backing off its send
     * <li><b>and</b> those partitions are not muted (to prevent reordering if
     *   {@value org.apache.kafka.clients.producer.ProducerConfig#MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION}
     *   is set to one)</li>
     * <li><b>and <i>any</i></b> of the following are true</li>
     * <ul>
     *     <li>The record set is full</li>
     *     <li>The record set has sat in the accumulator for at least lingerMs milliseconds</li>
     *     <li>The accumulator is out of memory and threads are blocking waiting for data (in this case all partitions
     *     are immediately considered ready).</li>
     *     <li>The accumulator has been closed</li>
     * </ul>
     * </ol>
     */
    // 获取待发送的消息
    public ReadyCheckResult ready(Cluster cluster, long nowMs) {
        // 记录可发送消息记录的topic-partition的leader节点
        Set<Node> readyNodes = new HashSet<>();
        // 记录触发下次检查待发送消息时需要等待的时间
        long nextReadyCheckDelayMs = Long.MAX_VALUE;
        // 记录是否存在不知道leader节点的topic-partition
        boolean unknownLeadersExist = false;
        // 记录是否有等待分配空间的线程(即当前BufferPool已用尽)
        boolean exhausted = this.free.queued() > 0;
        // 遍历所有的要发送到topic-partition的消息
        for (Map.Entry<TopicPartition, Deque<RecordBatch>> entry : this.batches.entrySet()) {
            TopicPartition part = entry.getKey();
            Deque<RecordBatch> deque = entry.getValue();
            // 获取当前分区的leader节点
            Node leader = cluster.leaderFor(part);
            // 不知道leader节点
            if (leader == null) {
                // 当前分区的leaser节点未知
                unknownLeadersExist = true;
            } else if (!readyNodes.contains(leader) && !muted.contains(part)) {
                synchronized (deque) {
                    // 获取topic-partition第一个RecordBatch，这里每次调用只获取每个topic-partition的第一个
                    RecordBatch batch = deque.peekFirst();
                    if (batch != null) {
                        // 记录当前RecordBatch是否为重试并且已符合再次发送的要求(最后一次放入消息的时间戳 + 重试间隔 > 当前时间)
                        boolean backingOff = batch.attempts > 0 && batch.lastAttemptMs + retryBackoffMs > nowMs;
                        // 记录当前RecordBatch已等待时间waitedTimeMs = 当前时间 - 最后一次放入消息的时间戳
                        long waitedTimeMs = nowMs - batch.lastAttemptMs;
                        // 记录当前RecordBatch需要等待的时间 timeToWaitMs = 如果是重试消息则记录retryBackoffMs，如果不是则记录lingerMs
                        long timeToWaitMs = backingOff ? retryBackoffMs : lingerMs;
                        // 记录当前RecordBatch消息剩余等待时间timeLeftMs
                        long timeLeftMs = Math.max(timeToWaitMs - waitedTimeMs, 0);
                        // 记录当前topic-partition的deque数量是否超过1个或者队列中第一个RecordBatch中的MemoryRecords已准备发送
                        boolean full = deque.size() > 1 || batch.records.isFull();
                        // 记录当前RecordBatch是否超时
                        boolean expired = waitedTimeMs >= timeToWaitMs;
                        // 最终总结下来就是方法注释中的四种场景就会触发当前RecordBatch需要发送出去
                        boolean sendable = full || expired || exhausted || closed || flushInProgress();
                        // RecordBatch可发送 && (非重试 || (重试 && 重试时间已到))
                        if (sendable && !backingOff) {
                            readyNodes.add(leader);
                        } else {
                            // Note that this results in a conservative estimate since an un-sendable partition may have
                            // a leader that will later be found to have sendable data. However, this is good enough
                            // since we'll just wake up and then sleep again for the remaining time.
                            // RecordBatch不能发送时，需要记录下来RecordBatch触发发送条件需要等待多久
                            // 场景一：RecordBatch为重试记录，并且重试时间未到
                            // 场景二：RecordBatch为非重试记录，并且等待时间未超过lingerMs配置
                            // 最终取轮询结果中的最小值
                            nextReadyCheckDelayMs = Math.min(timeLeftMs, nextReadyCheckDelayMs);
                        }
                    }
                }
            }
        }
        // 返回结果：可发送消息的Topic的leader所在的node节点，下次触发扫描需要等待的时间，是否存在不知到leader的Topic
        return new ReadyCheckResult(readyNodes, nextReadyCheckDelayMs, unknownLeadersExist);
    }

    /**
     * 判断RecordAccumulator中是否有未发送的消息记录
     * @return Whether there is any unsent record in the accumulator.
     */
    public boolean hasUnsent() {
        for (Map.Entry<TopicPartition, Deque<RecordBatch>> entry : this.batches.entrySet()) {
            Deque<RecordBatch> deque = entry.getValue();
            synchronized (deque) {
                if (!deque.isEmpty())
                    return true;
            }
        }
        return false;
    }

    /**
     * Drain all the data for the given nodes and collate them into a list of batches that will fit within the specified
     * size on a per-node basis. This method attempts to avoid choosing the same topic-node over and over.
     * 
     * @param cluster The current cluster metadata
     * @param nodes The list of node to drain
     * @param maxSize The maximum number of bytes to drain
     * @param now The current unix time in milliseconds
     * @return A list of {@link RecordBatch} for each node specified with total size less than the requested maxSize.
     */
    public Map<Integer, List<RecordBatch>> drain(Cluster cluster,
                                                 Set<Node> nodes,// 待发送消息的leader节点
                                                 int maxSize,
                                                 long now) {
        if (nodes.isEmpty())
            return Collections.emptyMap();

        Map<Integer, List<RecordBatch>> batches = new HashMap<>();
        // 遍历所有传进来的leader节点
        for (Node node : nodes) {
            // 累加当前node中消息大小
            int size = 0;
            // 获取leader节点上所有的partition
            List<PartitionInfo> parts = cluster.partitionsForNode(node.id());
            List<RecordBatch> ready = new ArrayList<>();
            /* to make starvation less likely this loop doesn't start at 0 */
            // drainIndex是全局遍历，记录了上次遍历leader节点后停止的位置
            // 当再次执行该方法时，可以继续往下遍历其他partition，因为可能存在一个leader上存在多个partition的情况
            int start = drainIndex = drainIndex % parts.size();
            // 遍历leader节点下所有的partition，每次从start位置开始
            do {
                PartitionInfo part = parts.get(drainIndex);
                TopicPartition tp = new TopicPartition(part.topic(), part.partition());
                // Only proceed if the partition has no in-flight batches.
                // TODO:
                if (!muted.contains(tp)) {
                    // 获取当前topic的Deque<RecordBatch>
                    Deque<RecordBatch> deque = getDeque(new TopicPartition(part.topic(), part.partition()));
                    if (deque != null) {
                        synchronized (deque) {
                            // 查看第一个RecordBatch，注意只是peek一下
                            RecordBatch first = deque.peekFirst();
                            if (first != null) {
                                boolean backoff = first.attempts > 0 && first.lastAttemptMs + retryBackoffMs > now;
                                // Only drain the batch if it is not during backoff period.
                                // 非重试消息 或者 是重试消息&&已到重发时间
                                if (!backoff) {
                                    // 这里的if判断就是当前node的累加消息大小已超过可发送最大值，则跳出循环
                                    // 剩余消息等待下次发送
                                    if (size + first.records.sizeInBytes() > maxSize && !ready.isEmpty()) {
                                        // there is a rare case that a single batch size is larger than the request size due
                                        // to compression; in this case we will still eventually send this batch in a single
                                        // request
                                        break;
                                    } else {
                                        // 累加消息到batch中，这里不是peek操作，而是poll操作，真正的出队
                                        RecordBatch batch = deque.pollFirst();
                                        batch.records.close();
                                        size += batch.records.sizeInBytes();
                                        ready.add(batch);
                                        batch.drainedMs = now;
                                    }
                                }
                            }
                        }
                    }
                }
                this.drainIndex = (this.drainIndex + 1) % parts.size();
            } while (start != drainIndex);
            // 记录每一次遍历leader节点后的List<RecordBatch>
            // 一个leader节点上可能存在多个partition,每次只取对应partition对应的Deque<RecordBatch>中的pollFirst
            batches.put(node.id(), ready);
        }
        return batches;
    }

    private Deque<RecordBatch> getDeque(TopicPartition tp) {
        return batches.get(tp);
    }

    /**
     * Get the deque for the given topic-partition, creating it if necessary.
     * 获取发送到某topic-partition的消息集合Deque
     */
    private Deque<RecordBatch> getOrCreateDeque(TopicPartition tp) {
        Deque<RecordBatch> d = this.batches.get(tp);
        if (d != null)
            return d;
        d = new ArrayDeque<>();
        Deque<RecordBatch> previous = this.batches.putIfAbsent(tp, d);
        if (previous == null)
            return d;
        else
            return previous;
    }

    /**
     * Deallocate the record batch
     * 释放消息累加器的已用空间
     */
    public void deallocate(RecordBatch batch) {
        incomplete.remove(batch);
        free.deallocate(batch.records.buffer(), batch.records.initialCapacity());
    }
    
    /**
     * Are there any threads currently waiting on a flush?
     *
     * package private for test
     */
    boolean flushInProgress() {
        return flushesInProgress.get() > 0;
    }

    /* Visible for testing */
    Map<TopicPartition, Deque<RecordBatch>> batches() {
        return Collections.unmodifiableMap(batches);
    }
    
    /**
     * Initiate the flushing of data from the accumulator...this makes all requests immediately ready
     */
    public void beginFlush() {
        this.flushesInProgress.getAndIncrement();
    }

    /**
     * Are there any threads currently appending messages?
     */
    private boolean appendsInProgress() {
        return appendsInProgress.get() > 0;
    }

    /**
     * Mark all partitions as ready to send and block until the send is complete
     */
    public void awaitFlushCompletion() throws InterruptedException {
        try {
            for (RecordBatch batch : this.incomplete.all())
                batch.produceFuture.await();
        } finally {
            this.flushesInProgress.decrementAndGet();
        }
    }

    /**
     * This function is only called when sender is closed forcefully. It will fail all the
     * incomplete batches and return.
     * while循环丢弃待发送的消息
     *
     */
    public void abortIncompleteBatches() {
        // We need to keep aborting the incomplete batch until no thread is trying to append to
        // 1. Avoid losing batches.
        // 2. Free up memory in case appending threads are blocked on buffer full.
        // This is a tight loop but should be able to get through very quickly.
        do {
            abortBatches();
        } while (appendsInProgress());
        // After this point, no thread will append any messages because they will see the close
        // flag set. We need to do the last abort after no thread was appending in case there was a new
        // batch appended by the last appending thread.
        abortBatches();
        this.batches.clear();
    }

    /**
     * Go through incomplete batches and abort them.
     * 检查不完整的批次并中止它们，也就是检查待发送的消息记录，然后删除掉
     */
    private void abortBatches() {
        for (RecordBatch batch : incomplete.all()) {
            Deque<RecordBatch> dq = getDeque(batch.topicPartition);
            // Close the batch before aborting
            synchronized (dq) {
                batch.records.close();
                dq.remove(batch);
            }
            batch.done(-1L, Record.NO_TIMESTAMP, new IllegalStateException("Producer is closed forcefully."));
            deallocate(batch);
        }
    }

    public void mutePartition(TopicPartition tp) {
        muted.add(tp);
    }

    public void unmutePartition(TopicPartition tp) {
        muted.remove(tp);
    }

    /**
     * Close this accumulator and force all the record buffers to be drained
     */
    public void close() {
        this.closed = true;
    }

    /*
     * Metadata about a record just appended to the record accumulator
     */
    public final static class RecordAppendResult {
        public final FutureRecordMetadata future;
        public final boolean batchIsFull;
        public final boolean newBatchCreated;

        public RecordAppendResult(FutureRecordMetadata future, boolean batchIsFull, boolean newBatchCreated) {
            this.future = future;
            this.batchIsFull = batchIsFull;
            this.newBatchCreated = newBatchCreated;
        }
    }

    /*
     * The set of nodes that have at least one complete record batch in the accumulator
     * 在累加器中至少有一个完整的RecordBatch需要发送
     */
    public final static class ReadyCheckResult {
        // 可发送消息的topic的leader节点
        public final Set<Node> readyNodes;
        // 下次扫描可发送消息需要等待的时间
        public final long nextReadyCheckDelayMs;
        // 是否有未知leader节点的topic
        public final boolean unknownLeadersExist;

        public ReadyCheckResult(Set<Node> readyNodes, long nextReadyCheckDelayMs, boolean unknownLeadersExist) {
            this.readyNodes = readyNodes;
            this.nextReadyCheckDelayMs = nextReadyCheckDelayMs;
            this.unknownLeadersExist = unknownLeadersExist;
        }
    }
    
    /*
     * A threadsafe helper class to hold RecordBatches that haven't been ack'd yet
     * 记录还没被ACK的消息
     */
    private final static class IncompleteRecordBatches {
        // 记录有等待发送消息的RecordBatch
        private final Set<RecordBatch> incomplete;

        public IncompleteRecordBatches() {
            this.incomplete = new HashSet<RecordBatch>();
        }
        
        public void add(RecordBatch batch) {
            synchronized (incomplete) {
                this.incomplete.add(batch);
            }
        }
        
        public void remove(RecordBatch batch) {
            synchronized (incomplete) {
                boolean removed = this.incomplete.remove(batch);
                if (!removed)
                    throw new IllegalStateException("Remove from the incomplete set failed. This should be impossible.");
            }
        }
        
        public Iterable<RecordBatch> all() {
            synchronized (incomplete) {
                return new ArrayList<>(this.incomplete);
            }
        }
    }

}
