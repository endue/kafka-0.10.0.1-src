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
package org.apache.kafka.clients.producer.internals;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.Time;


/**
 * A pool of ByteBuffers kept under a given memory limit. This class is fairly specific to the needs of the producer. In
 * particular it has the following properties:
 * <ol>
 * <li>There is a special "poolable size" and buffers of this size are kept in a free list and recycled
 * <li>It is fair. That is all memory is given to the longest waiting thread until it has sufficient memory. This
 * prevents starvation or deadlock when a thread asks for a large chunk of memory and needs to block until multiple
 * buffers are deallocated.
 * </ol>
 */
public final class BufferPool {
    // 记录BufferPool总大小，默认32M
    private final long totalMemory;
    // 记录ByteBuffer初始化大小 默认16kb
    private final int poolableSize;
    // 操作BufferPool的锁
    private final ReentrantLock lock;
    // 记录大小为poolableSize的空闲ByteBuffer
    private final Deque<ByteBuffer> free;
    // 记录等待获取内存空间的线程
    private final Deque<Condition> waiters;
    // 记录BufferPool剩余空闲的可用大小，默认初始化为32M(不包括free中的空闲空间)
    private long availableMemory;
    private final Metrics metrics;
    private final Time time;
    private final Sensor waitTime;

    /**
     * Create a new buffer pool
     * 
     * @param memory The maximum amount of memory that this buffer pool can allocate
     * @param poolableSize The buffer size to cache in the free list rather than deallocating
     * @param metrics instance of Metrics
     * @param time time instance
     * @param metricGrpName logical group name for metrics
     */
    public BufferPool(long memory, int poolableSize, Metrics metrics, Time time, String metricGrpName) {
        this.poolableSize = poolableSize;
        this.lock = new ReentrantLock();
        this.free = new ArrayDeque<ByteBuffer>();
        this.waiters = new ArrayDeque<Condition>();
        this.totalMemory = memory;
        this.availableMemory = memory;
        this.metrics = metrics;
        this.time = time;
        this.waitTime = this.metrics.sensor("bufferpool-wait-time");
        MetricName metricName = metrics.metricName("bufferpool-wait-ratio",
                                                   metricGrpName,
                                                   "The fraction of time an appender waits for space allocation.");
        this.waitTime.add(metricName, new Rate(TimeUnit.NANOSECONDS));
    }

    /**
     * Allocate a buffer of the given size. This method blocks if there is not enough memory and the buffer pool
     * is configured with blocking mode.
     * 根据参数size，申请一个ByteBuffer，如果没有足够的size，那么该方法会阻塞
     * 
     * @param size The buffer size to allocate in bytes
     * @param maxTimeToBlockMs The maximum time in milliseconds to block for buffer memory to be available
     * @return The buffer
     * @throws InterruptedException If the thread is interrupted while blocked
     * @throws IllegalArgumentException if size is larger than the total memory controlled by the pool (and hence we would block
     *         forever)
     */
    // 从BufferPool中申请size大小的空间
    // 首先根据参数size判断是否申请poolableSize大小，如果是那么尝试从Deque<ByteBuffer> free中获取
    // 如果不是poolableSize大小或Deque<ByteBuffer> free中没有足够的空间那么尝试将free中的空间释放给availableMemory，然后判断是否满足
    // 如果还是不满足，那就是没有足够的空间了，此时需要阻塞等待直到被唤醒、中断、超时
    public ByteBuffer allocate(int size, long maxTimeToBlockMs) throws InterruptedException {
        if (size > this.totalMemory)
            throw new IllegalArgumentException("Attempt to allocate " + size
                                               + " bytes, but there is a hard limit of "
                                               + this.totalMemory
                                               + " on memory allocations.");

        this.lock.lock();
        try {
            // check if we have a free buffer of the right size pooled
            // 申请空间正好为poolableSize(默认16kb) 并且 free不为空，直接从free中获取即可
            if (size == poolableSize && !this.free.isEmpty())
                return this.free.pollFirst();

            /* 执行到这里说明：
             * 1.size == poolableSize，不成立
             * 2.size == poolableSize成立，free为空
             **/
            // now check if the request is immediately satisfiable with the
            // memory on hand or if we need to block
            // 判断剩余的内存空间是否满足需要分配的空间

            // 计算Deque<ByteBuffer>中记录的空闲缓存
            int freeListSize = this.free.size() * this.poolableSize;
            if (this.availableMemory + freeListSize >= size) {// 满足
                // we have enough unallocated or pooled memory to immediately
                // satisfy the request
                // 释放Deque<ByteBuffer>中的空闲缓存到availableMemory中
                freeUp(size);
                // 更新空闲的内存，也就是分配size大小的空间出去
                this.availableMemory -= size;
                lock.unlock();
                // 返回分配的ByteBuffer
                return ByteBuffer.allocate(size);
            } else {// 不满足
                // we are out of memory and will have to block 内存空间不够，阻塞
                int accumulated = 0;
                // 初始化要分配的ByteBuffer
                ByteBuffer buffer = null;
                // 获取当前线程的Condition
                Condition moreMemory = this.lock.newCondition();
                // 计算剩余可等待的时间
                long remainingTimeToBlockNs = TimeUnit.MILLISECONDS.toNanos(maxTimeToBlockMs);
                // 将当前线程的Condition记录到waiters中
                this.waiters.addLast(moreMemory);
                // loop over and over until we have a buffer or have reserved
                // enough memory to allocate one
                // while死循环
                while (accumulated < size) {
                    long startWaitNs = time.nanoseconds();
                    long timeNs;
                    // 记录等待是否超时
                    boolean waitingTimeElapsed;
                    try {
                        // 调用当前线程的Condition.await方法阻塞当前线程
                        // 直到超时或被其他线程唤醒、中断
                        waitingTimeElapsed = !moreMemory.await(remainingTimeToBlockNs, TimeUnit.NANOSECONDS);
                    } catch (InterruptedException e) {
                        this.waiters.remove(moreMemory);
                        throw e;
                    } finally {
                        long endWaitNs = time.nanoseconds();
                        timeNs = Math.max(0L, endWaitNs - startWaitNs);
                        this.waitTime.record(timeNs, time.milliseconds());
                    }
                    // 当前线程被唤醒
                    // 判断当前线程是否等待超时
                    if (waitingTimeElapsed) {
                        this.waiters.remove(moreMemory);
                        throw new TimeoutException("Failed to allocate memory within the configured max blocking time " + maxTimeToBlockMs + " ms.");
                    }
                    // 如果被唤醒后没有超时，那么计算剩余可等待时间
                    remainingTimeToBlockNs -= timeNs;
                    // check if we can satisfy this request from the free list,
                    // otherwise allocate memory
                    // 获取要分配的空间：
                    // 场景1：线程A申请poolableSize大小
                    // 结果：会一直等待直到free有空闲空间
                    // 场景2：线程A申请非poolableSize大小
                    // 结果：free不为空则从free取出内存累加到availableMemory中，如果free为空或者availableMemory≥size跳出死循环
                    //      这时如果分配的空间依旧不够，会继续上面的while循环并记录已分配的内存到accumulated中

                    // 如果accumulated==0(没有申请到空间) && 申请的大小是poolableSize && free不为空
                    // 那么直接从free中拿一个
                    if (accumulated == 0 && size == this.poolableSize && !this.free.isEmpty()) {
                        // just grab a buffer from the free list
                        buffer = this.free.pollFirst();
                        accumulated = size;
                    } else {

                        // we'll need to allocate memory, but we may only get
                        // part of what we need on this iteration
                        // 释放Deque<ByteBuffer>中的空闲缓存到availableMemory中
                        freeUp(size - accumulated);
                        // 经过freeUp后，计算获取的空闲空间
                        int got = (int) Math.min(size - accumulated, this.availableMemory);
                        // 从availableMemory中剔除got
                        this.availableMemory -= got;
                        // 累加已经分配到的空间
                        accumulated += got;
                    }
                }

                // remove the condition for this thread to let the next thread
                // in line start getting memory
                // 校验
                Condition removed = this.waiters.removeFirst();
                if (removed != moreMemory)
                    throw new IllegalStateException("Wrong condition: this shouldn't happen.");

                // signal any additional waiters if there is more memory left
                // over for them
                // 如果还有剩余的空间，那么尝试从waiters中拿出某个线程的Condition并唤醒该线程
                if (this.availableMemory > 0 || !this.free.isEmpty()) {
                    if (!this.waiters.isEmpty())
                        this.waiters.peekFirst().signal();
                }

                // unlock and return the buffer
                lock.unlock();
                if (buffer == null)
                    return ByteBuffer.allocate(size);
                else
                    return buffer;
            }
        } finally {
            if (lock.isHeldByCurrentThread())
                lock.unlock();
        }
    }

    /**
     * Attempt to ensure we have at least the requested number of bytes of memory for allocation by deallocating pooled
     * buffers (if needed)
     */
    private void freeUp(int size) {
        // 如果free不为空并且availableMemory小于size
        // 不断取出free中的ByteBuffer容量累加到availableMemory直到availableMemory≥size
        while (!this.free.isEmpty() && this.availableMemory < size)
            this.availableMemory += this.free.pollLast().capacity();
    }

    /**
     * Return buffers to the pool. If they are of the poolable size add them to the free list, otherwise just mark the
     * memory as free.
     * 释放一个ByteBuffer到Deque<ByteBuffer> free或availableMemory
     * 
     * @param buffer The buffer to return
     * @param size The size of the buffer to mark as deallocated, note that this maybe smaller than buffer.capacity
     *             since the buffer may re-allocate itself during in-place compression
     */
    public void deallocate(ByteBuffer buffer, int size) {
        lock.lock();
        try {
            // 如果释放的ByteBuffer大小整好为poolableSize(16kb)
            // 将ByteBuffer加入free列表
            if (size == this.poolableSize && size == buffer.capacity()) {
                buffer.clear();
                this.free.add(buffer);
            } else {
                this.availableMemory += size;
            }
            // 唤醒等待获取ByteBuffer的线程
            Condition moreMem = this.waiters.peekFirst();
            if (moreMem != null)
                moreMem.signal();
        } finally {
            lock.unlock();
        }
    }

    public void deallocate(ByteBuffer buffer) {
        deallocate(buffer, buffer.capacity());
    }

    /**
     * the total free memory both unallocated and in the free list
     */
    public long availableMemory() {
        lock.lock();
        try {
            return this.availableMemory + this.free.size() * this.poolableSize;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get the unallocated memory (not in the free list or in use)
     */
    public long unallocatedMemory() {
        lock.lock();
        try {
            return this.availableMemory;
        } finally {
            lock.unlock();
        }
    }

    /**
     * The number of threads blocked waiting on memory
     */
    public int queued() {
        lock.lock();
        try {
            return this.waiters.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * The buffer size that will be retained in the free list after use
     */
    public int poolableSize() {
        return this.poolableSize;
    }

    /**
     * The total memory managed by this pool
     */
    public long totalMemory() {
        return this.totalMemory;
    }

    // package-private method used only for testing
    Deque<Condition> waiters() {
        return this.waiters;
    }
}
