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

import java.io._
import java.util.concurrent.TimeUnit

import kafka.utils._

import scala.collection._
import kafka.common.{KafkaException, TopicAndPartition}
import kafka.server.{BrokerState, OffsetCheckpoint, RecoveringFromUncleanShutdown}
import java.util.concurrent.{ExecutionException, ExecutorService, Executors, Future}

/**
 * The entry point to the kafka log management subsystem. The log manager is responsible for log creation, retrieval, and cleaning.
 * All read and write operations are delegated to the individual log instances.
 * 
 * The log manager maintains logs in one or more directories. New logs are created in the data directory
 * with the fewest logs. No attempt is made to move partitions after the fact or balance based on
 * size or I/O rate.
 * 
 * A background thread handles log retention by periodically truncating excess log segments.
 */
@threadsafe
class LogManager(val logDirs: Array[File],// 日志目录列表，加载的是"log.dirs"为空则加载"log.dir"
                 val topicConfigs: Map[String, LogConfig],// log配置
                 val defaultConfig: LogConfig,
                 val cleanerConfig: CleanerConfig,
                 ioThreads: Int,// 默认1
                 val flushCheckMs: Long,// flush检查时间 Long.MaxValue
                 val flushCheckpointMs: Long,// flushCheckpoint检查时间 60000
                 val retentionCheckMs: Long,// 日志保留检查时间 5 * 60 * 1000L
                 scheduler: Scheduler,// 任务调度线程池
                 val brokerState: BrokerState,// broker状态
                 private val time: Time) extends Logging {
  val RecoveryPointCheckpointFile = "recovery-point-offset-checkpoint"
  val LockFile = ".lock"
  val InitialTaskDelayMs = 30*1000
  private val logCreationOrDeletionLock = new Object
  // 底层基于ConcurrentHashMap,key是TopicAndPartition，value是log
  // 记录topic-partition<-->log关系的map也就是用于管理TopicAndPartition和Log之间的对应关系
  private val logs = new Pool[TopicAndPartition, Log]()
  // 检查日志目录不存在则创建
  createAndValidateLogDirs(logDirs)
  // 对所有的日志目录生成对应的FileLock
  private val dirLocks = lockLogDirs(logDirs)
  // logDirs是指“log.dirs”中配置的多个日志目录
  // RecoveryPointCheckpointFile记录日志目录下所有topic-partition日志已经刷新到磁盘的位置
  private val recoveryPointCheckpoints = logDirs.map(dir => (dir, new OffsetCheckpoint(new File(dir, RecoveryPointCheckpointFile)))).toMap
  // 加载日志
  loadLogs()

  // public, so we can access this from kafka.admin.DeleteTopicTest
  // 创建一个LogCleaner
  val cleaner: LogCleaner =
    // 是否启动清理调度任务
    if(cleanerConfig.enableCleaner)
      new LogCleaner(cleanerConfig, logDirs, logs, time = time)
    else
      null
  
  /**
   * Create and check validity of the given directories, specifically:
    * 检查日志目录
   * <ol>
   * <li> Ensure that there are no duplicates in the directory list
    * 确保目录列表中没有重复项
   * <li> Create each directory if it doesn't exist
    * 如果每个目录不存在，就创建它
   * <li> Check that each path is a readable directory
    * 检查每个路径是否是一个可读的目录
   * </ol>
   */
  private def createAndValidateLogDirs(dirs: Seq[File]) {
    if(dirs.map(_.getCanonicalPath).toSet.size < dirs.size)
      throw new KafkaException("Duplicate log directory found: " + logDirs.mkString(", "))
    // 遍历日志目录
    for(dir <- dirs) {
      // 如果日志目录不存在
      if(!dir.exists) {
        info("Log directory '" + dir.getAbsolutePath + "' not found, creating it.")
        // 创建
        val created = dir.mkdirs()
        // 创建失败
        if(!created)
          throw new KafkaException("Failed to create data directory " + dir.getAbsolutePath)
      }
      // 日志目录不是文件夹或不可读抛出异常
      if(!dir.isDirectory || !dir.canRead)
        throw new KafkaException(dir.getAbsolutePath + " is not a readable log directory.")
    }
  }
  
  /**
   * Lock all the given directories
    * 锁住所有的文件目录
    *
   */
  private def lockLogDirs(dirs: Seq[File]): Seq[FileLock] = {
    // 遍历目录然后生成
    dirs.map { dir =>
      // 将目录转为FileLock
      val lock = new FileLock(new File(dir, LockFile))
      // 尝试加锁并返回
      if(!lock.tryLock())
        throw new KafkaException("Failed to acquire lock on file .lock in " + lock.file.getParentFile.getAbsolutePath + 
                               ". A Kafka instance in another process or thread is using this directory.")
      lock
    }
  }
  
  /**
   * Recover and load all logs in the given data directories
    * 恢复并加载给定日志目录
   */
  private def loadLogs(): Unit = {
    info("Loading logs.")

    val threadPools = mutable.ArrayBuffer.empty[ExecutorService]
    val jobs = mutable.Map.empty[File, Seq[Future[_]]]
    // 遍历日志目录
    for (dir <- this.logDirs) {
      // 一个日志目录对应一个线程池，默认ioThreads = 1
      val pool = Executors.newFixedThreadPool(ioThreads)
      threadPools.append(pool)
      // 创建".kafka_cleanshutdown"文件
      val cleanShutdownFile = new File(dir, Log.CleanShutdownFile)
      // .kafka_cleanshutdown文件存在，说明已经开始恢复日志了
      if (cleanShutdownFile.exists) {
        // 打印日志，跳过所有日志的恢复工作
        debug(
          "Found clean shutdown file. " +
          "Skipping recovery for all logs in data directory: " +
          dir.getAbsolutePath)
      // .kafka_cleanshutdown文件不存在
      } else {
        // log recovery itself is being performed by `Log` class during initialization
        // 设置brokerState状态为RecoveringFromUncleanShutdown
        brokerState.newState(RecoveringFromUncleanShutdown)
      }
      // 获取对应topic-partition的checkpoint
      var recoveryPoints = Map[TopicAndPartition, Long]()
      try {
        // 基于recovery-point-offset-checkpoint文件
        recoveryPoints = this.recoveryPointCheckpoints(dir).read
      } catch {
        case e: Exception => {
          warn("Error occured while reading recovery-point-offset-checkpoint file of directory " + dir, e)
          warn("Resetting the recovery checkpoint to 0")
        }
      }

      val jobsForDir = for {
        // 获取日志目录dir下的所有文件
        dirContent <- Option(dir.listFiles).toList
        // 过滤出文件夹(此时一个文件夹对应的就是一个topic-partition)，然后处理
        logDir <- dirContent if logDir.isDirectory
      } yield {
        // 每个topic-partition对应一个线程
        CoreUtils.runnable {
          debug("Loading log '" + logDir.getName + "'")
          // 根据logDir获取对应的topic和partition
          val topicPartition = Log.parseTopicPartitionName(logDir)
          // 获取topic对应的配置
          val config = topicConfigs.getOrElse(topicPartition.topic, defaultConfig)
          // 获取对应log的recoveryPoint也就是offset
          val logRecoveryPoint = recoveryPoints.getOrElse(topicPartition, 0L)
          // 创建Log，同时会执行kafka.log.Log.loadSegments()加载所有的Segment
          val current = new Log(logDir, config, logRecoveryPoint, scheduler, time)
          // 保存Log
          val previous = this.logs.put(topicPartition, current)

          if (previous != null) {
            throw new IllegalArgumentException(
              "Duplicate log directories found: %s, %s!".format(
              current.dir.getAbsolutePath, previous.dir.getAbsolutePath))
          }
        }
      }
      // 提交任务，记录结果到jobs
      jobs(cleanShutdownFile) = jobsForDir.map(pool.submit).toSeq
    }


    try {
      // 遍历并获取日志目录dir下的结果，然后删除.kafka_cleanshutdown文件
      for ((cleanShutdownFile, dirJobs) <- jobs) {
        dirJobs.foreach(_.get)
        cleanShutdownFile.delete()
      }
    } catch {
      case e: ExecutionException => {
        error("There was an error in one of the threads during logs loading: " + e.getCause)
        throw e.getCause
      }
    } finally {
      threadPools.foreach(_.shutdown())
    }

    info("Logs loading complete.")
  }

  /**
   *  Start the background threads to flush logs and do log cleanup
    *  启动logManager
   */
  def startup() {
    /* Schedule the cleanup task to delete old logs */
    if(scheduler != null) {
      info("Starting log cleanup with a period of %d ms.".format(retentionCheckMs))
      // 旧的日志段删除任务,延迟30s执行，之后每5分钟执行一次
      scheduler.schedule("kafka-log-retention", 
                         cleanupLogs, 
                         delay = InitialTaskDelayMs, //  30*1000
                         period = retentionCheckMs, 
                         TimeUnit.MILLISECONDS)
      info("Starting log flusher with a default period of %d ms.".format(flushCheckMs))
      // 刷盘任务
      scheduler.schedule("kafka-log-flusher", 
                         flushDirtyLogs, 
                         delay = InitialTaskDelayMs, // 30*1000
                         period = flushCheckMs, 
                         TimeUnit.MILLISECONDS)
      // 检查点任务，记录了最后一次刷新的offset，表示多少日志已经落盘到磁盘上
      scheduler.schedule("kafka-recovery-point-checkpoint",
                         checkpointRecoveryPointOffsets,
                         delay = InitialTaskDelayMs,
                         period = flushCheckpointMs,
                         TimeUnit.MILLISECONDS)
    }
    // 如果设置为true，自动清理compaction类型的topic
    if(cleanerConfig.enableCleaner)
      cleaner.startup()
  }

  /**
   * Close all the logs
   */
  def shutdown() {
    info("Shutting down.")

    val threadPools = mutable.ArrayBuffer.empty[ExecutorService]
    val jobs = mutable.Map.empty[File, Seq[Future[_]]]

    // stop the cleaner first
    if (cleaner != null) {
      CoreUtils.swallow(cleaner.shutdown())
    }

    // close logs in each dir
    for (dir <- this.logDirs) {
      debug("Flushing and closing logs at " + dir)

      val pool = Executors.newFixedThreadPool(ioThreads)
      threadPools.append(pool)

      val logsInDir = logsByDir.getOrElse(dir.toString, Map()).values

      val jobsForDir = logsInDir map { log =>
        CoreUtils.runnable {
          // flush the log to ensure latest possible recovery point
          log.flush()
          log.close()
        }
      }

      jobs(dir) = jobsForDir.map(pool.submit).toSeq
    }


    try {
      for ((dir, dirJobs) <- jobs) {
        dirJobs.foreach(_.get)

        // update the last flush point
        debug("Updating recovery points at " + dir)
        checkpointLogsInDir(dir)

        // mark that the shutdown was clean by creating marker file
        debug("Writing clean shutdown marker at " + dir)
        CoreUtils.swallow(new File(dir, Log.CleanShutdownFile).createNewFile())
      }
    } catch {
      case e: ExecutionException => {
        error("There was an error in one of the threads during LogManager shutdown: " + e.getCause)
        throw e.getCause
      }
    } finally {
      threadPools.foreach(_.shutdown())
      // regardless of whether the close succeeded, we need to unlock the data directories
      dirLocks.foreach(_.destroy())
    }

    info("Shutdown complete.")
  }


  /**
   * Truncate the partition logs to the specified offsets and checkpoint the recovery point to this offset
   *
   * @param partitionAndOffsets Partition logs that need to be truncated
   */
  def truncateTo(partitionAndOffsets: Map[TopicAndPartition, Long]) {
    for ((topicAndPartition, truncateOffset) <- partitionAndOffsets) {
      val log = logs.get(topicAndPartition)
      // If the log does not exist, skip it
      if (log != null) {
        //May need to abort and pause the cleaning of the log, and resume after truncation is done.
        val needToStopCleaner: Boolean = (truncateOffset < log.activeSegment.baseOffset)
        if (needToStopCleaner && cleaner != null)
          cleaner.abortAndPauseCleaning(topicAndPartition)
        log.truncateTo(truncateOffset)
        if (needToStopCleaner && cleaner != null) {
          cleaner.maybeTruncateCheckpoint(log.dir.getParentFile, topicAndPartition, log.activeSegment.baseOffset)
          cleaner.resumeCleaning(topicAndPartition)
        }
      }
    }
    checkpointRecoveryPointOffsets()
  }

  /**
   *  Delete all data in a partition and start the log at the new offset
   *  @param newOffset The new offset to start the log with
   */
  def truncateFullyAndStartAt(topicAndPartition: TopicAndPartition, newOffset: Long) {
    val log = logs.get(topicAndPartition)
    // If the log does not exist, skip it
    if (log != null) {
        //Abort and pause the cleaning of the log, and resume after truncation is done.
      if (cleaner != null)
        cleaner.abortAndPauseCleaning(topicAndPartition)
      log.truncateFullyAndStartAt(newOffset)
      if (cleaner != null) {
        cleaner.maybeTruncateCheckpoint(log.dir.getParentFile, topicAndPartition, log.activeSegment.baseOffset)
        cleaner.resumeCleaning(topicAndPartition)
      }
    }
    checkpointRecoveryPointOffsets()
  }

  /**
   * Write out the current recovery point for all logs to a text file in the log directory 
   * to avoid recovering the whole log on startup.
    * 定时将每一个Log的recoveryPoint写入RecoveryPointCheckpoint文件
   */
  def checkpointRecoveryPointOffsets() {
    // 遍历所有的日志目录，获取每个日志目录的File文件
    this.logDirs.foreach(checkpointLogsInDir)
  }

  /**
   * Make a checkpoint for all logs in provided directory.
    * 为log创建检查点
   */
  private def checkpointLogsInDir(dir: File): Unit = {
    val recoveryPoints = this.logsByDir.get(dir.toString)
    if (recoveryPoints.isDefined) {
      // 对日志目录下的所有的topic-partiton，写入检查点文件
      this.recoveryPointCheckpoints(dir).write(recoveryPoints.get.mapValues(_.recoveryPoint))
    }
  }

  /**
   * Get the log if it exists, otherwise return None
   */
  def getLog(topicAndPartition: TopicAndPartition): Option[Log] = {
    val log = logs.get(topicAndPartition)
    if (log == null)
      None
    else
      Some(log)
  }

  /**
   * Create a log for the given topic and the given partition
   * If the log already exists, just return a copy of the existing log
   */
  def createLog(topicAndPartition: TopicAndPartition, config: LogConfig): Log = {
    logCreationOrDeletionLock synchronized {
      var log = logs.get(topicAndPartition)
      
      // check if the log has already been created in another thread
      if(log != null)
        return log
      
      // if not, create it
      val dataDir = nextLogDir()
      val dir = new File(dataDir, topicAndPartition.topic + "-" + topicAndPartition.partition)
      dir.mkdirs()
      log = new Log(dir, 
                    config,
                    recoveryPoint = 0L,
                    scheduler,
                    time)
      logs.put(topicAndPartition, log)
      info("Created log for partition [%s,%d] in %s with properties {%s}."
           .format(topicAndPartition.topic, 
                   topicAndPartition.partition, 
                   dataDir.getAbsolutePath,
                   {import JavaConversions._; config.originals.mkString(", ")}))
      log
    }
  }

  /**
   *  Delete a log.
   */
  def deleteLog(topicAndPartition: TopicAndPartition) {
    var removedLog: Log = null
    logCreationOrDeletionLock synchronized {
      removedLog = logs.remove(topicAndPartition)
    }
    if (removedLog != null) {
      //We need to wait until there is no more cleaning task on the log to be deleted before actually deleting it.
      if (cleaner != null) {
        cleaner.abortCleaning(topicAndPartition)
        cleaner.updateCheckpoints(removedLog.dir.getParentFile)
      }
      removedLog.delete()
      info("Deleted log for partition [%s,%d] in %s."
           .format(topicAndPartition.topic,
                   topicAndPartition.partition,
                   removedLog.dir.getAbsolutePath))
    }
  }

  /**
   * Choose the next directory in which to create a log. Currently this is done
   * by calculating the number of partitions in each directory and then choosing the
   * data directory with the fewest partitions.
   */
  private def nextLogDir(): File = {
    if(logDirs.size == 1) {
      logDirs(0)
    } else {
      // count the number of logs in each parent directory (including 0 for empty directories
      val logCounts = allLogs.groupBy(_.dir.getParent).mapValues(_.size)
      val zeros = logDirs.map(dir => (dir.getPath, 0)).toMap
      var dirCounts = (zeros ++ logCounts).toBuffer
    
      // choose the directory with the least logs in it
      val leastLoaded = dirCounts.sortBy(_._2).head
      new File(leastLoaded._1)
    }
  }

  /**
   * Runs through the log removing segments older than a certain age
    * 删除过期的segment
   */
  private def cleanupExpiredSegments(log: Log): Int = {
    // 如果日志永久保留，则不删除
    // retention.ms默认 24 * 7 *  * 60 * 60 * 1000L
    if (log.config.retentionMs < 0)
      return 0
    val startMs = time.milliseconds
    // 遍历log目录下的所有Segment，如果startMs - Segment的lastModified > log.config.retentionMs就删除
    // 这里传入的是一个predicate
    log.deleteOldSegments(startMs - _.lastModified > log.config.retentionMs)
  }

  /**
   *  Runs through the log removing segments until the size of the log
   *  is at least logRetentionSize bytes in size
    * 根据log的大小决定是否删除最旧的segment
   */
  private def cleanupSegmentsToMaintainSize(log: Log): Int = {
    // 1.retention.bytes配置的 < 0
    // 2.当前log大小 < retention.bytes
    // 以上情况不处理
    if(log.config.retentionSize < 0 || log.size < log.config.retentionSize)
      return 0
    // 计算当前log大小和retention.bytes的差值
    var diff = log.size - log.config.retentionSize
    // 定义一个predicate
    def shouldDelete(segment: LogSegment) = {
      if(diff - segment.size >= 0) {
        diff -= segment.size
        true
      } else {
        false
      }
    }
    // 循环删除segment，直到diff - segment.size < 0
    log.deleteOldSegments(shouldDelete)
  }

  /**
   * Delete any eligible logs. Return the number of segments deleted.
    * 删除所有符合条件的日志。返回删除的段数
   */
  def cleanupLogs() {
    debug("Beginning log cleanup...")
    var total = 0
    val startMs = time.milliseconds
    // 遍历所有的日志目录下topic-partition对应的log
    // 如果log的清理策略cleanup.policy设置为非compact
    for(log <- allLogs; if !log.config.compact) {
      debug("Garbage collecting '" + log.name + "'")
      total += cleanupExpiredSegments(log) + cleanupSegmentsToMaintainSize(log)
    }
    debug("Log cleanup completed. " + total + " files deleted in " +
                  (time.milliseconds - startMs) / 1000 + " seconds")
  }

  /**
   * Get all the partition logs
   */
  def allLogs(): Iterable[Log] = logs.values

  /**
   * Get a map of TopicAndPartition => Log
   */
  def logsByTopicPartition: Map[TopicAndPartition, Log] = logs.toMap

  /**
   * Map of log dir to logs by topic and partitions in that dir
   */
  private def logsByDir = {
    this.logsByTopicPartition.groupBy {
      case (_, log) => log.dir.getParent
    }
  }

  /**
   * Flush any log which has exceeded its flush interval and has unwritten messages.
    * 消息刷盘
   */
  private def flushDirtyLogs() = {
    debug("Checking for dirty logs to flush...")
    // 遍历所有的日志目录，获取每个topic-partition对应的log
    for ((topicAndPartition, log) <- logs) {
      try {
        // 获取log最后一次执行flush操作的时间
        val timeSinceLastFlush = time.milliseconds - log.lastFlushTime
        debug("Checking if flush is needed on " + topicAndPartition.topic + " flush interval  " + log.config.flushMs +
              " last flushed " + log.lastFlushTime + " time since last flush: " + timeSinceLastFlush)
        // 超过指定时间flush.ms，执行flush操作
        if(timeSinceLastFlush >= log.config.flushMs)
          log.flush
      } catch {
        case e: Throwable =>
          error("Error flushing topic " + topicAndPartition.topic, e)
      }
    }
  }
}
