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
package kafka.utils.timer

import java.util.concurrent.{DelayQueue, Executors, ThreadFactory, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock

import kafka.utils.threadsafe
import org.apache.kafka.common.utils.Utils

trait Timer {
  /**
    * Add a new task to this executor. It will be executed after the task's delay
    * (beginning from the time of submission)
    * @param timerTask the task to add
    */
  def add(timerTask: TimerTask): Unit

  /**
    * Advance the internal clock, executing any tasks whose expiration has been
    * reached within the duration of the passed timeout.
    * @param timeoutMs
    * @return whether or not any tasks were executed
    */
  def advanceClock(timeoutMs: Long): Boolean

  /**
    * Get the number of tasks pending execution
    * @return the number of tasks
    */
  def size: Int

  /**
    * Shutdown the timer service, leaving pending tasks unexecuted
    */
  def shutdown(): Unit
}

/**
  * 延迟功能的定时器
  * @param executorName
  * @param tickMs
  * @param wheelSize
  * @param startMs
  */
@threadsafe
class SystemTimer(executorName: String,
                  tickMs: Long = 1,
                  wheelSize: Int = 20,
                  startMs: Long = System.currentTimeMillis) extends Timer {

  // timeout timer
  private[this] val taskExecutor = Executors.newFixedThreadPool(1, new ThreadFactory() {
    def newThread(runnable: Runnable): Thread =
      Utils.newThread("executor-"+executorName, runnable, false)
  })

  //
  private[this] val delayQueue = new DelayQueue[TimerTaskList]()
  // 任务数
  private[this] val taskCounter = new AtomicInteger(0)
  // 时间轮
  private[this] val timingWheel = new TimingWheel(
    tickMs = tickMs,
    wheelSize = wheelSize,
    startMs = startMs,
    taskCounter = taskCounter,
    delayQueue
  )

  // Locks used to protect data structures while ticking
  private[this] val readWriteLock = new ReentrantReadWriteLock()
  private[this] val readLock = readWriteLock.readLock()
  private[this] val writeLock = readWriteLock.writeLock()

  /**
    * 添加延迟任务
    * @param timerTask the task to add
    */
  def add(timerTask: TimerTask): Unit = {
    readLock.lock()
    try {
      // 将任务封装为TimerTaskEntry
      addTimerTaskEntry(new TimerTaskEntry(timerTask, timerTask.delayMs + System.currentTimeMillis()))
    } finally {
      readLock.unlock()
    }
  }

  /**
    * 添加延迟任务
    * @param timerTaskEntry
    */
  private def addTimerTaskEntry(timerTaskEntry: TimerTaskEntry): Unit = {
    // 返回false表示任务已过期和已取消
    if (!timingWheel.add(timerTaskEntry)) {
      // Already expired or cancelled
      // 任务没有取消但是已过期，那么需要立即执行当前任务
      if (!timerTaskEntry.cancelled)
        taskExecutor.submit(timerTaskEntry.timerTask)
    }
  }

  private[this] val reinsert = (timerTaskEntry: TimerTaskEntry) => addTimerTaskEntry(timerTaskEntry)

  /*
   * Advances the clock if there is an expired bucket. If there isn't any expired bucket when called,
   * waits up to timeoutMs before giving up.
   * 推进时间轮指针的前进
   */
  def advanceClock(timeoutMs: Long): Boolean = {
    // 阻塞200ms，获取超时的TimerTaskList
    var bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS)
    // 不为null，准备处理超时的TimerTaskList
    if (bucket != null) {
      writeLock.lock()
      try {
        while (bucket != null) {
          // 修改时间轮的currentTime为当前槽的过期时间
          timingWheel.advanceClock(bucket.getExpiration())
          // 处理TimerTaskList中的任务
          bucket.flush(reinsert)
          // 继续处理超时的TimerTaskList，非阻塞
          bucket = delayQueue.poll()
        }
      } finally {
        writeLock.unlock()
      }
      true
    } else {
      false
    }
  }

  def size: Int = taskCounter.get

  override def shutdown() {
    taskExecutor.shutdown()
  }

}

