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

package kafka.log

import java.io.File
import kafka.metrics.KafkaMetricsGroup
import com.yammer.metrics.core.Gauge
import kafka.utils.{Logging, Pool}
import kafka.server.OffsetCheckpoint
import collection.mutable
import java.util.concurrent.locks.ReentrantLock
import kafka.utils.CoreUtils._
import java.util.concurrent.TimeUnit
import kafka.common.{LogCleaningAbortedException, TopicAndPartition}

private[log] sealed trait LogCleaningState
private[log] case object LogCleaningInProgress extends LogCleaningState
private[log] case object LogCleaningAborted extends LogCleaningState
private[log] case object LogCleaningPaused extends LogCleaningState

/**
 *  Manage the state of each partition being cleaned.
 *  If a partition is to be cleaned, it enters the LogCleaningInProgress state.
 *  While a partition is being cleaned, it can be requested to be aborted and paused. Then the partition first enters
 *  the LogCleaningAborted state. Once the cleaning task is aborted, the partition enters the LogCleaningPaused state.
 *  While a partition is in the LogCleaningPaused state, it won't be scheduled for cleaning again, until cleaning is
 *  requested to be resumed.
 */
/**
  *
  * @param logDirs 日志目录列表，加载的是"log.dirs"为空则加载"log.dir"
  * @param logs // 记录topic-partition<-->log关系的map也就是用于管理TopicAndPartition和Log之间的对应关系
  */
private[log] class LogCleanerManager(val logDirs: Array[File], val logs: Pool[TopicAndPartition, Log]) extends Logging with KafkaMetricsGroup {
  
  override val loggerName = classOf[LogCleaner].getName

  // package-private for testing
  // 每一个日志目录下都有一个cleaner-offset-checkpoint文件
  private[log] val offsetCheckpointFile = "cleaner-offset-checkpoint"
  
  /* the offset checkpoints holding the last cleaned point for each log */
  // Map[File,OffsetCheckpoint]类型，用来维护日志目录和cleaner-offset-checkpoint文件之间的对应关系
  // 每个日志目录文件夹都会存在一个cleaner-offset-checkpoint文件
  private val checkpoints = logDirs.map(dir => (dir, new OffsetCheckpoint(new File(dir, offsetCheckpointFile)))).toMap

  /* the set of logs currently being cleaned */
  // HashMap[TopicAndPartition, LogCleaningState]，记录正在进行的清理的TopicAndPartition的压缩状态
  // 记录要清理脏数据的topica-partition
  private val inProgress = mutable.HashMap[TopicAndPartition, LogCleaningState]()

  /* a global lock used to control all access to the in-progress set and the offset checkpoints */
  // 锁
  private val lock = new ReentrantLock
  
  /* for coordinating the pausing and the cleaning of a partition */
  // 暂停分区的清理
  private val pausedCleaningCond = lock.newCondition()
  
  /* a gauge for tracking the cleanable ratio of the dirtiest log */
  // 最脏的segment，其脏数据的占比
  @volatile private var dirtiestLogCleanableRatio = 0.0
  newGauge("max-dirty-percent", new Gauge[Int] { def value = (100 * dirtiestLogCleanableRatio).toInt })

  /**
   * @return the position processed for all logs.
    * 获取topic-partition对应的cleaner-offset-checkpoint
   */
  def allCleanerCheckpoints(): Map[TopicAndPartition, Long] =
    checkpoints.values.flatMap(_.read()).toMap

   /**
    * Choose the log to clean next and add it to the in-progress set. We recompute this
    * every time off the full set of logs to allow logs to be dynamically added to the pool of logs
    * the log manager maintains.
     * 过滤配置文件中配置的日志目录下所有的topic-partition对应的log，
     * 找到脏数据最大的topic-partition,添加到inProgress集合中准备进行清理
    */
  def grabFilthiestLog(): Option[LogToClean] = {
    inLock(lock) {
      // 获取每个日志目录下记录的所有topic-partition的cleaner-offset-checkpoint,返回Map类型
      // 文件中起始记录的是下次清理的startOffset,上次清理完后的offset + 1
      val lastClean: Map[TopicAndPartition, Long] = allCleanerCheckpoints()
      // 遍历日志目录过滤掉一些数据，将符合的topic-partiton对应的log封装成一个LogToClean
      // 筛选要清理的log
      val dirtyLogs: Iterable[LogToClean] = logs.filter {
        // 筛选类型为Compact
        case (topicAndPartition, log) => log.config.compact  // skip any logs marked for delete rather than dedupe
      }.filterNot {
        // 筛选inProgress(Map类型)不包含的
        case (topicAndPartition, log) => inProgress.contains(topicAndPartition) // skip any logs already in-progress
      }.map {
        // 处理要清理的log，为其创建一个LogToClean实例
        case (topicAndPartition, log) => // create a LogToClean instance for each
          // if the log segments are abnormally truncated and hence the checkpointed offset
          // is no longer valid, reset to the log starting offset and log the error event
          // 获取topic-partition对应的所有logSegment中第一个的baseOffset
          val logStartOffset = log.logSegments.head.baseOffset
          // 计算第一个需要压缩的位置firstDirtyOffset(也就是在firstDirtyOffset之前都是已经被压缩过的)
          val firstDirtyOffset = {
            // 获取已清理到的位置，也就是checkpoints中记录的对应topic-partition的offset，如果不存在则初始化为刚刚计算的logStartOffset
            val offset = lastClean.getOrElse(topicAndPartition, logStartOffset)
            if (offset < logStartOffset) {
              error("Resetting first dirty offset to log start offset %d since the checkpointed offset %d is invalid."
                    .format(logStartOffset, offset))
              logStartOffset
            } else {
              offset
            }
          }
          // 创建一个对应的LogToClean
          LogToClean(topicAndPartition, log, firstDirtyOffset)
      }.filter(ltc => ltc.totalBytes > 0) // skip any empty logs 最后过滤出字节数 > 0的topic-partiton对应的log的LogToClean

      // 计算最脏topic-partition的log的LogToClean其脏数据的占比
      this.dirtiestLogCleanableRatio = if (!dirtyLogs.isEmpty) dirtyLogs.max.cleanableRatio else 0
      // and must meet the minimum threshold for dirty byte ratio
      // "min.cleanable.dirty.ratio" 默认0.5，过滤脏数据占比超过50%的LogToClean
      val cleanableLogs: Iterable[LogToClean] = dirtyLogs.filter(ltc => ltc.cleanableRatio > ltc.log.config.minCleanableRatio)
      // 没有要清理的，返回Node
      if(cleanableLogs.isEmpty) {
        None
      // 如果有要清理的,获取脏数据占比最大的记录到inProgress然后返回
      } else {
        // 记录要清理日志的topic-partition，状态为LogCleaningInProgress
        val filthiest: LogToClean = cleanableLogs.max
        inProgress.put(filthiest.topicPartition, LogCleaningInProgress)
        Some(filthiest)
      }
    }
  }

  /**
   *  Abort the cleaning of a particular partition, if it's in progress. This call blocks until the cleaning of
   *  the partition is aborted.
   *  This is implemented by first abortAndPausing and then resuming the cleaning of the partition.
   */
  def abortCleaning(topicAndPartition: TopicAndPartition) {
    inLock(lock) {
      abortAndPauseCleaning(topicAndPartition)
      resumeCleaning(topicAndPartition)
    }
    info("The cleaning for partition %s is aborted".format(topicAndPartition))
  }

  /**
   *  Abort the cleaning of a particular partition if it's in progress, and pause any future cleaning of this partition.
   *  This call blocks until the cleaning of the partition is aborted and paused.
   *  1. If the partition is not in progress, mark it as paused.
   *  2. Otherwise, first mark the state of the partition as aborted.
   *  3. The cleaner thread checks the state periodically and if it sees the state of the partition is aborted, it
   *     throws a LogCleaningAbortedException to stop the cleaning task.
   *  4. When the cleaning task is stopped, doneCleaning() is called, which sets the state of the partition as paused.
   *  5. abortAndPauseCleaning() waits until the state of the partition is changed to paused.
   */
  def abortAndPauseCleaning(topicAndPartition: TopicAndPartition) {
    inLock(lock) {
      inProgress.get(topicAndPartition) match {
        case None =>
          inProgress.put(topicAndPartition, LogCleaningPaused)
        case Some(state) =>
          state match {
            case LogCleaningInProgress =>
              inProgress.put(topicAndPartition, LogCleaningAborted)
            case s =>
              throw new IllegalStateException("Compaction for partition %s cannot be aborted and paused since it is in %s state."
                                              .format(topicAndPartition, s))
          }
      }
      while (!isCleaningInState(topicAndPartition, LogCleaningPaused))
        pausedCleaningCond.await(100, TimeUnit.MILLISECONDS)
    }
    info("The cleaning for partition %s is aborted and paused".format(topicAndPartition))
  }

  /**
   *  Resume the cleaning of a paused partition. This call blocks until the cleaning of a partition is resumed.
    *  恢复对暂停的分区的清理。这个调用阻塞，直到恢复分区清理
   */
  def resumeCleaning(topicAndPartition: TopicAndPartition) {
    inLock(lock) {
      inProgress.get(topicAndPartition) match {
        case None =>
          throw new IllegalStateException("Compaction for partition %s cannot be resumed since it is not paused."
                                          .format(topicAndPartition))
        case Some(state) =>
          state match {
            case LogCleaningPaused =>
              inProgress.remove(topicAndPartition)
            case s =>
              throw new IllegalStateException("Compaction for partition %s cannot be resumed since it is in %s state."
                                              .format(topicAndPartition, s))
          }
      }
    }
    info("Compaction for partition %s is resumed".format(topicAndPartition))
  }

  /**
   *  Check if the cleaning for a partition is in a particular state. The caller is expected to hold lock while making the call.
   */
  private def isCleaningInState(topicAndPartition: TopicAndPartition, expectedState: LogCleaningState): Boolean = {
    inProgress.get(topicAndPartition) match {
      case None => false
      case Some(state) =>
        if (state == expectedState)
          true
        else
          false
    }
  }

  /**
   *  Check if the cleaning for a partition is aborted. If so, throw an exception.
    *  检查对分区的清理是否已中止。如果是，抛出一个异常
   */
  def checkCleaningAborted(topicAndPartition: TopicAndPartition) {
    inLock(lock) {
      if (isCleaningInState(topicAndPartition, LogCleaningAborted))
        throw new LogCleaningAbortedException()
    }
  }

  /**
    * 更新topic-partiton对应的cleaner-offset-checkpoint
    * @param dataDir
    * @param update
    */
  def updateCheckpoints(dataDir: File, update: Option[(TopicAndPartition,Long)]) {
    inLock(lock) {
      val checkpoint = checkpoints(dataDir)
      val existing = checkpoint.read().filterKeys(logs.keys) ++ update
      checkpoint.write(existing)
    }
  }

  // 截断checkpoint文件
  def maybeTruncateCheckpoint(dataDir: File, topicAndPartition: TopicAndPartition, offset: Long) {
    inLock(lock) {
      // 判断对应topic-partiton的日志类型是否为compact
      if (logs.get(topicAndPartition).config.compact) {
        // 获取对应的OffsetCheckpoint
        val checkpoint = checkpoints(dataDir)
        // 读取OffsetCheckpoint中的内容，返回类型为Map[TopicAndPartition, Long]
        val existing = checkpoint.read()
        // 获取对应topic-partition当前checkpoint的offset，没有就返回0
        // 如果写入的checkpoint的offset > 截断的目标offset
        if (existing.getOrElse(topicAndPartition, 0L) > offset)
          // 重新写入当前topic-partition最新的checkpoint offset
          checkpoint.write(existing + (topicAndPartition -> offset))
      }
    }
  }

  /**
   * Save out the endOffset and remove the given log from the in-progress set, if not aborted.
    * 保存偏移量并从正在进行的日志集中删除给定的日志(如果没有中止的话)
   */
  def doneCleaning(topicAndPartition: TopicAndPartition, dataDir: File, endOffset: Long) {
    inLock(lock) {
      inProgress(topicAndPartition) match {
          // 如果状态为LogCleaningInProgress
        case LogCleaningInProgress =>
          // 更新cleaner-offset-checkpoint并从inProgress删除
          updateCheckpoints(dataDir,Option(topicAndPartition, endOffset))
          inProgress.remove(topicAndPartition)
        case LogCleaningAborted =>
          inProgress.put(topicAndPartition, LogCleaningPaused)
          pausedCleaningCond.signalAll()
        case s =>
          throw new IllegalStateException("In-progress partition %s cannot be in %s state.".format(topicAndPartition, s))
      }
    }
  }
}
