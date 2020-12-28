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

import java.util.Iterator;
import java.util.PriorityQueue;

/**
 * Tracks a set of tasks to be executed after a delay.
 */
public class DelayedTaskQueue {

    private PriorityQueue<Entry> tasks;

    public DelayedTaskQueue() {
        tasks = new PriorityQueue<Entry>();
    }

    /**
     * Schedule a task for execution in the future.
     *
     * @param task the task to execute
     * @param at the time at which to
     */
    public void add(DelayedTask task, long at) {
        tasks.add(new Entry(task, at));
    }

    /**
     * Remove a task from the queue if it is present
     * @param task the task to be removed
     * @returns true if a task was removed as a result of this call
     * 删除所有task类型的任务
     */
    public boolean remove(DelayedTask task) {
        boolean wasRemoved = false;
        Iterator<Entry> iterator = tasks.iterator();
        while (iterator.hasNext()) {
            Entry entry = iterator.next();
            if (entry.task.equals(task)) {
                iterator.remove();
                wasRemoved = true;
            }
        }
        return wasRemoved;
    }

    /**
     * Get amount of time in milliseconds until the next event. Returns Long.MAX_VALUE if no tasks are scheduled.
     *
     * @return the remaining time in milliseconds
     */
    public long nextTimeout(long now) {
        if (tasks.isEmpty())
            return Long.MAX_VALUE;
        else
            return Math.max(tasks.peek().timeout - now, 0);
    }

    /**
     * Run any ready tasks.
     *
     * @param now the current time
     * 执行已到期的任务
     */
    public void poll(long now) {
        // 队列不为空 && 有到期的任务
        while (!tasks.isEmpty() && tasks.peek().timeout <= now) {
            // 获取任务
            Entry entry = tasks.poll();
            // 执行
            entry.task.run(now);
        }
    }

    // 延迟队列中记录的一个个包装延迟任务的对象
    private static class Entry implements Comparable<Entry> {
        // 延迟任务
        DelayedTask task;
        // 超时时间
        long timeout;

        public Entry(DelayedTask task, long timeout) {
            this.task = task;
            this.timeout = timeout;
        }

        @Override
        public int compareTo(Entry entry) {
            return Long.compare(timeout, entry.timeout);
        }
    }
}