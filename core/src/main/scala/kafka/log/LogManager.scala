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
/**
  * 调用参考{@link kafka.server.KafkaServer#createLogManager(org.I0Itec.zkclient.ZkClient, kafka.server.BrokerState)}
  * @param logDirs 在传递进来时就初始化好了相关的日志目录
  * @param topicConfigs
  * @param defaultConfig
  * @param cleanerConfig
  * @param ioThreads
  * @param flushCheckMs
  * @param flushCheckpointMs
  * @param retentionCheckMs
  * @param scheduler
  * @param brokerState
  * @param time
  */
@threadsafe
class LogManager(val logDirs: Array[File],// 日志目录列表，加载的是"log.dirs"为空则加载"log.dir",
                 val topicConfigs: Map[String, LogConfig],// topic的配置信息
                 val defaultConfig: LogConfig,// log默认配置信息
                 val cleanerConfig: CleanerConfig,// 清理日志所需配置信息
                 ioThreads: Int,// 默认1
                 val flushCheckMs: Long,// flush检查时间 Long.MaxValue
                 val flushCheckpointMs: Long,// flushCheckpoint检查时间 60000
                 val retentionCheckMs: Long,// 日志保留检查时间 5 * 60 * 1000L
                 scheduler: Scheduler,// 任务调度线程池
                 val brokerState: BrokerState,// broker状态
                 private val time: Time) extends Logging {
  // 总体初始化分如下几步:
  // 1.创建logDirs对应的所有日志目录
  // 2.在logDirs指向的每个日志目录中创建.lock文件并让FileLock包装
  // 3.创建logDirs指向的每个日志目录的OffsetCheckpoint对象,里面有recovery-point-offset-checkpoint文件
  // 4.恢复磁盘上日志文件
  val RecoveryPointCheckpointFile = "recovery-point-offset-checkpoint"
  val LockFile = ".lock"
  // 日志相关定义任务在执行首次任务时延迟的毫秒数
  val InitialTaskDelayMs = 30*1000
  // 删除或创建日志的锁
  private val logCreationOrDeletionLock = new Object
  // 底层基于ConcurrentHashMap,key是TopicAndPartition，value是Log
  // 记录topic-partition<-->log关系的map也就是用于管理TopicAndPartition和Log之间的对应关系
  private val logs = new Pool[TopicAndPartition, Log]()
  // 检查参数logDirs也就是日志目录
  // 不存在则创建
  createAndValidateLogDirs(logDirs)
  // 对所有的日志目录生成对应的FileLock
  private val dirLocks = lockLogDirs(logDirs)
  // 在logDirs(是指“log.dirs”中配置的多个日志目录)中所有的日志目录下创建"recovery-point-offset-checkpoint"文件
  // 该文件记录当前服务日志目录下所有topic-partition已经刷新到磁盘的位置
  // recoveryPointCheckpoints: Predef.Map[File, OffsetCheckpoint],key是日志文件目录,value是OffsetCheckpoint类
  private val recoveryPointCheckpoints = logDirs.map(dir => (dir, new OffsetCheckpoint(new File(dir, RecoveryPointCheckpointFile)))).toMap
  // 加载磁盘日志
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
        // 创建目录
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
    * 为所有的文件目录生成文件锁
    *
   */
  private def lockLogDirs(dirs: Seq[File]): Seq[FileLock] = {
    // 遍历目录然后生成FileLock
    dirs.map { dir =>
      // 给文件目录加锁,内部会在dir目录下创建一个LockFile文件
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
    * 当LogManager创建时
    * 恢复并加载给定日志目录里的日志
   */
  private def loadLogs(): Unit = {
    info("Loading logs.")
    // 初始化线程池数组ExecutorService
    val threadPools = mutable.ArrayBuffer.empty[ExecutorService]
    //
    val jobs = mutable.Map.empty[File, Seq[Future[_]]]
    // 遍历处理日志目录
    for (dir <- this.logDirs) {
      /**
        * 1. 一个日志目录对应一个线程池,线程池线程数默认ioThreads = 1,最后将线程池添加到threadPools中
        */
      val pool = Executors.newFixedThreadPool(ioThreads)
      threadPools.append(pool)
      /**
        * 2. 查找日志目录下的".kafka_cleanshutdown"文件
        */
      val cleanShutdownFile = new File(dir, Log.CleanShutdownFile)
      /**
        * 2.1. 如果存在,首先说明当前服务属于正常关闭并且当前日志目录中的数据已被正确完整处理.当前属于再次启动服务,参考".kafka_cleanshutdown"文件创建参考shutdown()方法.
        * 所以不需要对该日志目录下的日志做任何的处理
        */
      if (cleanShutdownFile.exists) {
        debug(
          "Found clean shutdown file. " +
          "Skipping recovery for all logs in data directory: " +
          dir.getAbsolutePath)
        /**
          * 2.2. 如果不存在,说明服务正常关闭或异常关闭时,当前日志目录中的数据没有得到正确的处理,设置broker状态为RecoveringFromUncleanShutdown
          */
      } else {
        // log recovery itself is being performed by `Log` class during initialization
        // 设置brokerState状态为RecoveringFromUncleanShutdown
        brokerState.newState(RecoveringFromUncleanShutdown)
      }
      /**
        * 3. 获取当前日志目录下记录的topic-partition的recoveryPoints
        * 基于recovery-point-offset-checkpoint文件,恢复当前日志目录下每个topic-partition对应的checkpoint
        */
      var recoveryPoints = Map[TopicAndPartition, Long]()
      try {
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
        // 过滤出所有的文件夹(此时一个文件夹对应的就是一个topic-partition的日志目录)，然后处理
        logDir <- dirContent if logDir.isDirectory
      } yield {
        // 每个topic-partition创建一个线程
        CoreUtils.runnable {
          debug("Loading log '" + logDir.getName + "'")
          // 根据logDir获取该目录对应的topic和partition
          val topicPartition = Log.parseTopicPartitionName(logDir)
          // 获取topic对应的配置
          val config = topicConfigs.getOrElse(topicPartition.topic, defaultConfig)
          // 获取topic-partition对应的checkpoint,也就是已刷入磁盘的offset
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
      // key是".kafka_cleanshutdown"对应的File文件
      // value是".kafka_cleanshutdown"文件对应的日志目录下的所有的topic-partition的恢复线程
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
      // 关闭所有的线程池
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
    // cleaner线程不为空，那么关闭它
    if (cleaner != null) {
      CoreUtils.swallow(cleaner.shutdown())
    }

    // close logs in each dir
    // 遍历所有的LogDirs
    for (dir <- this.logDirs) {
      debug("Flushing and closing logs at " + dir)

      val pool = Executors.newFixedThreadPool(ioThreads)
      threadPools.append(pool)

      val logsInDir = logsByDir.getOrElse(dir.toString, Map()).values
      // 创建任务，功能是获取目录下的Log对象，然后刷盘并关闭
      val jobsForDir = logsInDir map { log =>
        CoreUtils.runnable {
          // flush the log to ensure latest possible recovery point
          log.flush()
          log.close()
        }
      }
      // 提交任务到线程池
      jobs(dir) = jobsForDir.map(pool.submit).toSeq
    }


    try {
      // 遍历任务
      for ((dir, dirJobs) <- jobs) {
        // 获取结果
        dirJobs.foreach(_.get)

        // update the last flush point
        debug("Updating recovery points at " + dir)
        // 更新checkpoint
        checkpointLogsInDir(dir)

        // mark that the shutdown was clean by creating marker file
        debug("Writing clean shutdown marker at " + dir)
        // 写.kafka_cleanshutdown文件
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
      // 最后关闭所有的文件锁
      dirLocks.foreach(_.destroy())
    }

    info("Shutdown complete.")
  }


  /**
   * Truncate the partition logs to the specified offsets and checkpoint the recovery point to this offset
    * 将分区日志截断到指定的偏移量，并将恢复点检查点设置为该偏移量
   *
   * @param partitionAndOffsets Partition logs that need to be truncated 需要被截断的topic-partition
   */
  def truncateTo(partitionAndOffsets: Map[TopicAndPartition, Long]) {
    for ((topicAndPartition, truncateOffset) <- partitionAndOffsets) {
      // 获取对应topic-partition的Log实例
      val log = logs.get(topicAndPartition)
      // If the log does not exist, skip it
      if (log != null) {
        //May need to abort and pause the cleaning of the log, and resume after truncation is done.
        // 可能需要中止和暂停清理日志，并在截断完成后恢复
        // 如果被截取的offset小于活跃segmetn的baseOffset，那么说明此事可能存在cleaner线程
        val needToStopCleaner: Boolean = (truncateOffset < log.activeSegment.baseOffset)
        if (needToStopCleaner && cleaner != null)
          // 终止掉cleaner线程的执行
          cleaner.abortAndPauseCleaning(topicAndPartition)
        // 截断日志到指定的offset
        log.truncateTo(truncateOffset)
        // 如果暂停了清理日志，那么现在就给它恢复一些
        if (needToStopCleaner && cleaner != null) {
          // 节点checkpoint文件中记录的信息
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
      // 对日志目录下的所有Log的topic-partiton，写入检查点文件
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
    *  删除日志
   */
  def deleteLog(topicAndPartition: TopicAndPartition) {
    var removedLog: Log = null
    logCreationOrDeletionLock synchronized {
      // 从logs集合中删除对应topic-partition的Log实例
      removedLog = logs.remove(topicAndPartition)
    }
    if (removedLog != null) {
      //We need to wait until there is no more cleaning task on the log to be deleted before actually deleting it.
      // 如果开启了日志清理线程
      if (cleaner != null) {
        // 中断当前线程的清理
        cleaner.abortCleaning(topicAndPartition)
        // 更新cleaner-offset-checkpoint文件
        cleaner.updateCheckpoints(removedLog.dir.getParentFile)
      }
      // 删除该topic-partition日志目录下的.log和.index文件，最后在删除该日志目录
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
    // retention.ms默认 24 * 7 *  * 60 * 60 * 1000L,设置小于0表示永久保留
    if (log.config.retentionMs < 0)
      return 0
    // 获取当前时间戳
    val startMs = time.milliseconds
    // 遍历log目录下的所有Segment，如果当前时间戳startMs - Segment的lastModified > log.config.retentionMs就删除
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
    // 如果Log的清理策略cleanup.policy设置为非compact,默认Delete,那么就清理该Log
    for(log <- allLogs; if !log.config.compact) {
      debug("Garbage collecting '" + log.name + "'")
      total += cleanupExpiredSegments(log) + cleanupSegmentsToMaintainSize(log)
    }
    debug("Log cleanup completed. " + total + " files deleted in " +
                  (time.milliseconds - startMs) / 1000 + " seconds")
  }

  /**
   * Get all the partition logs
    * 获取topic-partition对应的Log对象
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
