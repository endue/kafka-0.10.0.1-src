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

import java.io.{DataOutputStream, File}
import java.nio._
import java.util.Date
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.yammer.metrics.core.Gauge
import kafka.common._
import kafka.message._
import kafka.metrics.KafkaMetricsGroup
import kafka.utils._

import scala.collection._

/**
 * The cleaner is responsible for removing obsolete records from logs which have the dedupe retention strategy.
 * A message with key K and offset O is obsolete if there exists a message with key K and offset O' such that O < O'.
 * 
 * Each log can be thought of being split into two sections of segments: a "clean" section which has previously been cleaned followed by a
 * "dirty" section that has not yet been cleaned. The active log segment is always excluded from cleaning.
 *
 * The cleaning is carried out by a pool of background threads. Each thread chooses the dirtiest log that has the "dedupe" retention policy 
 * and cleans that. The dirtiness of the log is guessed by taking the ratio of bytes in the dirty section of the log to the total bytes in the log. 
 * 
 * To clean a log the cleaner first builds a mapping of key=>last_offset for the dirty section of the log. See kafka.log.OffsetMap for details of
 * the implementation of the mapping. 
 * 
 * Once the key=>offset map is built, the log is cleaned by recopying each log segment but omitting any key that appears in the offset map with a 
 * higher offset than what is found in the segment (i.e. messages with a key that appears in the dirty section of the log).
 * 
 * To avoid segments shrinking to very small sizes with repeated cleanings we implement a rule by which if we will merge successive segments when
 * doing a cleaning if their log and index size are less than the maximum log and index size prior to the clean beginning.
 * 
 * Cleaned segments are swapped into the log as they become available.
 * 
 * One nuance that the cleaner must handle is log truncation. If a log is truncated while it is being cleaned the cleaning of that log is aborted.
 * 
 * Messages with null payload are treated as deletes for the purpose of log compaction. This means that they receive special treatment by the cleaner. 
 * The cleaner will only retain delete records for a period of time to avoid accumulating space indefinitely. This period of time is configurable on a per-topic
 * basis and is measured from the time the segment enters the clean portion of the log (at which point any prior message with that key has been removed).
 * Delete markers in the clean section of the log that are older than this time will not be retained when log segments are being recopied as part of cleaning.
 * 
 * @param config Configuration parameters for the cleaner
 * @param logDirs The directories where offset checkpoints reside
 * @param logs The pool of logs
 * @param time A way to control the passage of time
 */
// 清理日志
class LogCleaner(val config: CleanerConfig,
                 val logDirs: Array[File],// 日志目录列表，加载的是"log.dirs"为空则加载"log.dir"
                 val logs: Pool[TopicAndPartition, Log], // 记录topic-partition<-->log关系的map也就是用于管理TopicAndPartition和Log之间的对应关系
                 time: Time = SystemTime) extends Logging with KafkaMetricsGroup {
  
  /* for managing the state of partitions being cleaned. package-private to allow access in tests */
  // 用于负责每一个Log的压缩状态管理以及cleaner checkpoint的维护和更新
  // 记录每个主题的每个分区中已清理的偏移量
  private[log] val cleanerManager = new LogCleanerManager(logDirs, logs)

  /* a throttle used to limit the I/O of all the cleaner threads to a user-specified maximum rate */
  // 用于将所有cleaner线程的I/O限制为用户指定的最大速率 log.cleaner.io.max.bytes.per.second
  private val throttler = new Throttler(desiredRatePerSec = config.maxIoBytesPerSecond, 
                                        checkIntervalMs = 300, 
                                        throttleDown = true, 
                                        "cleaner-io",
                                        "bytes",
                                        time = time)
  
  /* the threads */
  // log.cleaner.threads默认1
  private val cleaners = (0 until config.numThreads).map(new CleanerThread(_))
  
  /* a metric to track the maximum utilization of any thread's buffer in the last cleaning */
  newGauge("max-buffer-utilization-percent", 
           new Gauge[Int] {
             def value: Int = cleaners.map(_.lastStats).map(100 * _.bufferUtilization).max.toInt
           })
  /* a metric to track the recopy rate of each thread's last cleaning */
  newGauge("cleaner-recopy-percent", 
           new Gauge[Int] {
             def value: Int = {
               val stats = cleaners.map(_.lastStats)
               val recopyRate = stats.map(_.bytesWritten).sum.toDouble / math.max(stats.map(_.bytesRead).sum, 1)
               (100 * recopyRate).toInt
             }
           })
  /* a metric to track the maximum cleaning time for the last cleaning from each thread */
  newGauge("max-clean-time-secs",
           new Gauge[Int] {
             def value: Int = cleaners.map(_.lastStats).map(_.elapsedSecs).max.toInt
           })
  
  /**
   * Start the background cleaning
    * 启动后台压缩日志线程
   */
  def startup() {
    info("Starting the log cleaner")
    cleaners.foreach(_.start())
  }
  
  /**
   * Stop the background cleaning
    * 关闭后台压缩日志线程
   */
  def shutdown() {
    info("Shutting down the log cleaner.")
    cleaners.foreach(_.shutdown())
  }
  
  /**
   *  Abort the cleaning of a particular partition, if it's in progress. This call blocks until the cleaning of
   *  the partition is aborted.
   */
  def abortCleaning(topicAndPartition: TopicAndPartition) {
    cleanerManager.abortCleaning(topicAndPartition)
  }

  /**
   * Update checkpoint file, removing topics and partitions that no longer exist
   */
  def updateCheckpoints(dataDir: File) {
    cleanerManager.updateCheckpoints(dataDir, update=None)
  }

  /**
   * Truncate cleaner offset checkpoint for the given partition if its checkpointed offset is larger than the given offset
   */
  def maybeTruncateCheckpoint(dataDir: File, topicAndPartition: TopicAndPartition, offset: Long) {
    cleanerManager.maybeTruncateCheckpoint(dataDir, topicAndPartition, offset)
  }

  /**
   *  Abort the cleaning of a particular partition if it's in progress, and pause any future cleaning of this partition.
   *  This call blocks until the cleaning of the partition is aborted and paused.
   */
  def abortAndPauseCleaning(topicAndPartition: TopicAndPartition) {
    cleanerManager.abortAndPauseCleaning(topicAndPartition)
  }

  /**
   *  Resume the cleaning of a paused partition. This call blocks until the cleaning of a partition is resumed.
   */
  def resumeCleaning(topicAndPartition: TopicAndPartition) {
    cleanerManager.resumeCleaning(topicAndPartition)
  }

  /**
   * For testing, a way to know when work has completed. This method waits until the
   * cleaner has processed up to the given offset on the specified topic/partition
   *
   * @param topic The Topic to be cleaned
   * @param part The partition of the topic to be cleaned
   * @param offset The first dirty offset that the cleaner doesn't have to clean
   * @param maxWaitMs The maximum time in ms to wait for cleaner
   *
   * @return A boolean indicating whether the work has completed before timeout
   */
  def awaitCleaned(topic: String, part: Int, offset: Long, maxWaitMs: Long = 60000L): Boolean = {
    def isCleaned = cleanerManager.allCleanerCheckpoints.get(TopicAndPartition(topic, part)).fold(false)(_ >= offset)
    var remainingWaitMs = maxWaitMs
    while (!isCleaned && remainingWaitMs > 0) {
      val sleepTime = math.min(100, remainingWaitMs)
      Thread.sleep(sleepTime)
      remainingWaitMs -= sleepTime
    }
    isCleaned
  }
  
  /**
   * The cleaner threads do the actual log cleaning. Each thread processes does its cleaning repeatedly by
   * choosing the dirtiest log, cleaning it, and then swapping in the cleaned segments.
    * 清理日志后台线程，doWork()
   */
  private class CleanerThread(threadId: Int)
    extends ShutdownableThread(name = "kafka-log-cleaner-thread-" + threadId, isInterruptible = false) {
    
    override val loggerName = classOf[LogCleaner].getName
    
    if(config.dedupeBufferSize / config.numThreads > Int.MaxValue)
      warn("Cannot use more than 2G of cleaner buffer space per cleaner thread, ignoring excess buffer space...")

    val cleaner = new Cleaner(id = threadId,// 线程ID，用于标识Cleaner
                              // log.cleaner.dedupe.buffer.size默认128 * 1024 * 1024L
                              // hashAlgorithm默认MD5
                              offsetMap = new SkimpyOffsetMap(memory = math.min(config.dedupeBufferSize / config.numThreads, Int.MaxValue).toInt, 
                                                              hashAlgorithm = config.hashAlgorithm),// 用于重复数据删除的map
                              // log.cleaner.io.buffer.size默认512 * 1024
                              ioBufferSize = config.ioBufferSize / config.numThreads / 2,// 要使用的缓冲区大小。内存使用量将是这个数字的两倍，因为有一个读写缓冲区
                              // message.max.bytes默认1000000 + 8 + 4
                              maxIoBufferSize = config.maxMessageSize,
                              // log.cleaner.io.buffer.load.factor默认0.9
                              dupBufferLoadFactor = config.dedupeBufferLoadFactor,
                              throttler = throttler,
                              time = time,
                              checkDone = checkDone)
    
    @volatile var lastStats: CleanerStats = new CleanerStats()
    private val backOffWaitLatch = new CountDownLatch(1)

    private def checkDone(topicAndPartition: TopicAndPartition) {
      if (!isRunning.get())
        throw new ThreadShutdownException
      cleanerManager.checkCleaningAborted(topicAndPartition)
    }

    /**
     * The main loop for the cleaner thread
      * 不断循环清理log
     */
    override def doWork() {
      cleanOrSleep()
    }
    
    
    override def shutdown() = {
    	 initiateShutdown()
    	 backOffWaitLatch.countDown()
    	 awaitShutdown()
     }
     
    /**
     * Clean a log if there is a dirty log available, otherwise sleep for a bit
      * 如果有可用的脏日志，则清理日志，否则休眠一段时间
     */
    private def cleanOrSleep() {
      // 选择要清理的topic-partiton对应的log，添加到inProgress集合状态为LogCleaningInProgress并返回
      cleanerManager.grabFilthiestLog() match {
        // 没有要清理的
        case None =>
          // there are no cleanable logs, sleep a while
          // 休眠 log.cleaner.backoff.ms = 15 * 1000
          backOffWaitLatch.await(config.backOffMs, TimeUnit.MILLISECONDS)
        // 有要清理的
        case Some(cleanable) =>
          // there's a log, clean it
          // 获取清理的起始位置
          var endOffset = cleanable.firstDirtyOffset
          try {
            // 开始清理,返回已清理的位置
            endOffset = cleaner.clean(cleanable)
            // 统计相关忽略
            recordStats(cleaner.id, cleanable.log.name, cleanable.firstDirtyOffset, endOffset, cleaner.stats)
          } catch {
            case pe: LogCleaningAbortedException => // task can be aborted, let it go.
          } finally {
            cleanerManager.doneCleaning(cleanable.topicPartition, cleanable.log.dir.getParentFile, endOffset)
          }
      }
    }
    
    /**
     * Log out statistics on a single run of the cleaner.
      * @param id
      * @param name
      * @param from
      * @param to
      * @param stats
    */
    def recordStats(id: Int, name: String, from: Long, to: Long, stats: CleanerStats) {
      this.lastStats = stats
      cleaner.statsUnderlying.swap
      def mb(bytes: Double) = bytes / (1024*1024)
      val message = 
        "%n\tLog cleaner thread %d cleaned log %s (dirty section = [%d, %d])%n".format(id, name, from, to) + 
        "\t%,.1f MB of log processed in %,.1f seconds (%,.1f MB/sec).%n".format(mb(stats.bytesRead), 
                                                                                stats.elapsedSecs, 
                                                                                mb(stats.bytesRead/stats.elapsedSecs)) + 
        "\tIndexed %,.1f MB in %.1f seconds (%,.1f Mb/sec, %.1f%% of total time)%n".format(mb(stats.mapBytesRead), 
                                                                                           stats.elapsedIndexSecs, 
                                                                                           mb(stats.mapBytesRead)/stats.elapsedIndexSecs, 
                                                                                           100 * stats.elapsedIndexSecs/stats.elapsedSecs) +
        "\tBuffer utilization: %.1f%%%n".format(100 * stats.bufferUtilization) +
        "\tCleaned %,.1f MB in %.1f seconds (%,.1f Mb/sec, %.1f%% of total time)%n".format(mb(stats.bytesRead), 
                                                                                           stats.elapsedSecs - stats.elapsedIndexSecs, 
                                                                                           mb(stats.bytesRead)/(stats.elapsedSecs - stats.elapsedIndexSecs), 100 * (stats.elapsedSecs - stats.elapsedIndexSecs).toDouble/stats.elapsedSecs) + 
        "\tStart size: %,.1f MB (%,d messages)%n".format(mb(stats.bytesRead), stats.messagesRead) +
        "\tEnd size: %,.1f MB (%,d messages)%n".format(mb(stats.bytesWritten), stats.messagesWritten) + 
        "\t%.1f%% size reduction (%.1f%% fewer messages)%n".format(100.0 * (1.0 - stats.bytesWritten.toDouble/stats.bytesRead), 
                                                                   100.0 * (1.0 - stats.messagesWritten.toDouble/stats.messagesRead))
      info(message)
      if (stats.invalidMessagesRead > 0) {
        warn("\tFound %d invalid messages during compaction.".format(stats.invalidMessagesRead))
      }
    }
   
  }
}

/**
 * This class holds the actual logic for cleaning a log
 * @param id An identifier used for logging
 * @param offsetMap The map used for deduplication
 * @param ioBufferSize The size of the buffers to use. Memory usage will be 2x this number as there is a read and write buffer.
 * @param maxIoBufferSize The maximum size of a message that can appear in the log
 * @param dupBufferLoadFactor The maximum percent full for the deduplication buffer
 * @param throttler The throttler instance to use for limiting I/O rate.
 * @param time The time instance
 * @param checkDone Check if the cleaning for a partition is finished or aborted.
 */
private[log] class Cleaner(val id: Int,
                           val offsetMap: OffsetMap,
                           ioBufferSize: Int,
                           maxIoBufferSize: Int,
                           dupBufferLoadFactor: Double,
                           throttler: Throttler,
                           time: Time,
                           checkDone: (TopicAndPartition) => Unit) extends Logging {
  
  override val loggerName = classOf[LogCleaner].getName

  this.logIdent = "Cleaner " + id + ": "
  
  /* cleaning stats - one instance for the current (or next) cleaning cycle and one for the last completed cycle */
  val statsUnderlying = (new CleanerStats(time), new CleanerStats(time))
  def stats = statsUnderlying._1

  /* buffer used for read i/o */
  // 读缓冲区
  private var readBuffer = ByteBuffer.allocate(ioBufferSize)
  
  /* buffer used for write i/o */
  // 写缓冲区
  private var writeBuffer = ByteBuffer.allocate(ioBufferSize)

  /**
   * Clean the given log
   *
   * @param cleanable The log to be cleaned
   *
   * @return The first offset not cleaned
   */
  private[log] def clean(cleanable: LogToClean): Long = {
    stats.clear()
    info("Beginning cleaning of log %s.".format(cleanable.log.name))
    // 获取要清理的log
    val log = cleanable.log

    // build the offset map
    info("Building offset map for %s...".format(cleanable.log.name))
    // 获取topic-partiton对应的log中当前活跃的Segment的baseOffset
    // 也就是清理操作只能清理到活跃Segment的上一个Segment
    val upperBoundOffset = log.activeSegment.baseOffset
    // 开始清理日志,处理log目录下一个个的segment，endOffset为清理到的位置
    // 返回值为已处理到的offset
    val endOffset = buildOffsetMap(log, cleanable.firstDirtyOffset, upperBoundOffset, offsetMap) + 1
    stats.indexDone()
    
    // figure out the timestamp below which it is safe to remove delete tombstones
    // this position is defined to be a configurable time beneath the last modified time of the last clean segment
    // 如果一条消息的key不为null，但是其value为null，那么此消息就是墓碑消息。
    // 日志清理线程发现墓碑消息时会先进行常规的清理，并保留墓碑消息一段时间。
    // 墓碑消息的保留条件是当前墓碑消息所在的日志分段的最近修改时间lastModifiedTime大于deleteHorizonMs，默认24 * 60 * 60 * 1000L
    val deleteHorizonMs = 
      log.logSegments(0, cleanable.firstDirtyOffset).lastOption match {
        case None => 0L
        case Some(seg) => seg.lastModified - log.config.deleteRetentionMs
    }
        
    // group the segments and clean the groups
    info("Cleaning log %s (discarding tombstones prior to %s)...".format(log.name, new Date(deleteHorizonMs)))
    // 分组需要清理的LogSegments
    // 分组逻辑就是累加各个Segment当满足segment.bytes或segment.index.bytes等时就作为一组，然后计算下一组
    for (group <- groupSegmentsBySize(log.logSegments(0, endOffset), log.config.segmentSize, log.config.maxIndexSize))
      // 清理
      cleanSegments(log, group, offsetMap, deleteHorizonMs)
      
    // record buffer utilization
    stats.bufferUtilization = offsetMap.utilization
    
    stats.allDone()

    endOffset
  }

  /**
   * Clean a group of segments into a single replacement segment
   *
   * @param log The log being cleaned
   * @param segments The group of segments being cleaned
   * @param map The offset map to use for cleaning segments
   * @param deleteHorizonMs The time to retain delete tombstones
   */
  private[log] def cleanSegments(log: Log,
                                 segments: Seq[LogSegment], 
                                 map: OffsetMap, 
                                 deleteHorizonMs: Long) {
    // create a new segment with the suffix .cleaned appended to both the log and index name
    val logFile = new File(segments.head.log.file.getPath + Log.CleanedFileSuffix)
    logFile.delete()
    val indexFile = new File(segments.head.index.file.getPath + Log.CleanedFileSuffix)
    indexFile.delete()
    val messages = new FileMessageSet(logFile, fileAlreadyExists = false, initFileSize = log.initFileSize(), preallocate = log.config.preallocate)
    val index = new OffsetIndex(indexFile, segments.head.baseOffset, segments.head.index.maxIndexSize)
    val cleaned = new LogSegment(messages, index, segments.head.baseOffset, segments.head.indexIntervalBytes, log.config.randomSegmentJitter, time)

    try {
      // clean segments into the new destination segment
      for (old <- segments) {
        val retainDeletes = old.lastModified > deleteHorizonMs
        info("Cleaning segment %s in log %s (last modified %s) into %s, %s deletes."
            .format(old.baseOffset, log.name, new Date(old.lastModified), cleaned.baseOffset, if(retainDeletes) "retaining" else "discarding"))
        cleanInto(log.topicAndPartition, old, cleaned, map, retainDeletes, log.config.messageFormatVersion.messageFormatVersion)
      }

      // trim excess index
      index.trimToValidSize()

      // flush new segment to disk before swap
      cleaned.flush()

      // update the modification date to retain the last modified date of the original files
      val modified = segments.last.lastModified
      cleaned.lastModified = modified

      // swap in new segment
      info("Swapping in cleaned segment %d for segment(s) %s in log %s.".format(cleaned.baseOffset, segments.map(_.baseOffset).mkString(","), log.name))
      //  新LogSegment替换清理操作前的LogSegments
      log.replaceSegments(cleaned, segments)
    } catch {
      case e: LogCleaningAbortedException =>
        cleaned.delete()
        throw e
    }
  }

  /**
   * Clean the given source log segment into the destination segment using the key=>offset mapping
   * provided
   *
   * @param source The dirty log segment
   * @param dest The cleaned log segment
   * @param map The key=>offset mapping
   * @param retainDeletes Should delete tombstones be retained while cleaning this segment
   * @param messageFormatVersion the message format version to use after compaction
   */
  private[log] def cleanInto(topicAndPartition: TopicAndPartition,
                             source: LogSegment,
                             dest: LogSegment,
                             map: OffsetMap,
                             retainDeletes: Boolean,
                             messageFormatVersion: Byte) {
    var position = 0
    while (position < source.log.sizeInBytes) {
      checkDone(topicAndPartition)
      // read a chunk of messages and copy any that are to be retained to the write buffer to be written out
      readBuffer.clear()
      writeBuffer.clear()
      val messages = new ByteBufferMessageSet(source.log.readInto(readBuffer, position))
      throttler.maybeThrottle(messages.sizeInBytes)
      // check each message to see if it is to be retained
      var messagesRead = 0
      for (entry <- messages.shallowIterator) {
        val size = MessageSet.entrySize(entry.message)
        stats.readMessage(size)
        if (entry.message.compressionCodec == NoCompressionCodec) {
          if (shouldRetainMessage(source, map, retainDeletes, entry)) {
            ByteBufferMessageSet.writeMessage(writeBuffer, entry.message, entry.offset)
            stats.recopyMessage(size)
          }
          messagesRead += 1
        } else {
          // We use the absolute offset to decide whether to retain the message or not. This is handled by the
          // deep iterator.
          val messages = ByteBufferMessageSet.deepIterator(entry)
          var writeOriginalMessageSet = true
          val retainedMessages = new mutable.ArrayBuffer[MessageAndOffset]
          messages.foreach { messageAndOffset =>
            messagesRead += 1
            if (shouldRetainMessage(source, map, retainDeletes, messageAndOffset))
              retainedMessages += messageAndOffset
            else writeOriginalMessageSet = false
          }

          // There are no messages compacted out, write the original message set back
          if (writeOriginalMessageSet)
            ByteBufferMessageSet.writeMessage(writeBuffer, entry.message, entry.offset)
          else
            compressMessages(writeBuffer, entry.message.compressionCodec, messageFormatVersion, retainedMessages)
        }
      }

      position += messages.validBytes
      // if any messages are to be retained, write them out
      if (writeBuffer.position > 0) {
        writeBuffer.flip()
        val retained = new ByteBufferMessageSet(writeBuffer)
        dest.append(retained.head.offset, retained)
        throttler.maybeThrottle(writeBuffer.limit)
      }
      
      // if we read bytes but didn't get even one complete message, our I/O buffer is too small, grow it and try again
      if (readBuffer.limit > 0 && messagesRead == 0)
        growBuffers()
    }
    restoreBuffers()
  }

  private def compressMessages(buffer: ByteBuffer,
                               compressionCodec: CompressionCodec,
                               messageFormatVersion: Byte,
                               messageAndOffsets: Seq[MessageAndOffset]) {
    require(compressionCodec != NoCompressionCodec, s"compressionCodec must not be $NoCompressionCodec")
    if (messageAndOffsets.nonEmpty) {
      val messages = messageAndOffsets.map(_.message)
      val magicAndTimestamp = MessageSet.magicAndLargestTimestamp(messages)
      val firstMessageOffset = messageAndOffsets.head
      val firstAbsoluteOffset = firstMessageOffset.offset
      var offset = -1L
      val timestampType = firstMessageOffset.message.timestampType
      val messageWriter = new MessageWriter(math.min(math.max(MessageSet.messageSetSize(messages) / 2, 1024), 1 << 16))
      messageWriter.write(codec = compressionCodec, timestamp = magicAndTimestamp.timestamp, timestampType = timestampType, magicValue = messageFormatVersion) { outputStream =>
        val output = new DataOutputStream(CompressionFactory(compressionCodec, messageFormatVersion, outputStream))
        try {
          for (messageOffset <- messageAndOffsets) {
            val message = messageOffset.message
            offset = messageOffset.offset
            if (messageFormatVersion > Message.MagicValue_V0) {
              // The offset of the messages are absolute offset, compute the inner offset.
              val innerOffset = messageOffset.offset - firstAbsoluteOffset
              output.writeLong(innerOffset)
            } else
              output.writeLong(offset)
            output.writeInt(message.size)
            output.write(message.buffer.array, message.buffer.arrayOffset, message.buffer.limit)
          }
        } finally {
          output.close()
        }
      }
      ByteBufferMessageSet.writeMessage(buffer, messageWriter, offset)
      stats.recopyMessage(messageWriter.size + MessageSet.LogOverhead)
    }
  }

  private def shouldRetainMessage(source: kafka.log.LogSegment,
                                  map: kafka.log.OffsetMap,
                                  retainDeletes: Boolean,
                                  entry: kafka.message.MessageAndOffset): Boolean = {
    val key = entry.message.key
    if (key != null) {
      val foundOffset = map.get(key)
      /* two cases in which we can get rid of a message:
       *   1) if there exists a message with the same key but higher offset
       *   2) if the message is a delete "tombstone" marker and enough time has passed
       */
      val redundant = foundOffset >= 0 && entry.offset < foundOffset
      val obsoleteDelete = !retainDeletes && entry.message.isNull
      !redundant && !obsoleteDelete
    } else {
      stats.invalidMessage()
      false
    }
  }

  /**
   * Double the I/O buffer capacity
   */
  def growBuffers() {
    if(readBuffer.capacity >= maxIoBufferSize || writeBuffer.capacity >= maxIoBufferSize)
      throw new IllegalStateException("This log contains a message larger than maximum allowable size of %s.".format(maxIoBufferSize))
    val newSize = math.min(this.readBuffer.capacity * 2, maxIoBufferSize)
    info("Growing cleaner I/O buffers from " + readBuffer.capacity + "bytes to " + newSize + " bytes.")
    this.readBuffer = ByteBuffer.allocate(newSize)
    this.writeBuffer = ByteBuffer.allocate(newSize)
  }
  
  /**
   * Restore the I/O buffer capacity to its original size
   */
  def restoreBuffers() {
    if(this.readBuffer.capacity > this.ioBufferSize)
      this.readBuffer = ByteBuffer.allocate(this.ioBufferSize)
    if(this.writeBuffer.capacity > this.ioBufferSize)
      this.writeBuffer = ByteBuffer.allocate(this.ioBufferSize)
  }

  /**
   * Group the segments in a log into groups totaling less than a given size. the size is enforced separately for the log data and the index data.
   * We collect a group of such segments together into a single
   * destination segment. This prevents segment sizes from shrinking too much.
   *
   * @param segments The log segments to group
   * @param maxSize the maximum size in bytes for the total of all log data in a group
   * @param maxIndexSize the maximum size in bytes for the total of all index data in a group
    * @return A list of grouped segments
    *  分组
    *  逻辑是遍历segments然后累加各个Segment当
    *  1.logSize > segment.bytes
    *  2.indexSize > segment.index.bytes
    *  3.消息条数 > int.maxvalue
    *  以上三种情况任一满足一个就作为一组，然后开始计算下一组数据
   */
  private[log] def groupSegmentsBySize(segments: Iterable[LogSegment], maxSize: Int, maxIndexSize: Int): List[Seq[LogSegment]] = {
    var grouped = List[List[LogSegment]]()
    // segments数组
    var segs = segments.toList
    while(!segs.isEmpty) {
      var group = List(segs.head)
      var logSize = segs.head.size
      var indexSize = segs.head.index.sizeInBytes
      segs = segs.tail
      while(!segs.isEmpty &&
            logSize + segs.head.size <= maxSize &&
            indexSize + segs.head.index.sizeInBytes <= maxIndexSize &&
            segs.head.index.lastOffset - group.last.index.baseOffset <= Int.MaxValue) {
        group = segs.head :: group
        logSize += segs.head.size
        indexSize += segs.head.index.sizeInBytes
        segs = segs.tail
      }
      grouped ::= group.reverse
    }
    grouped.reverse
  }

  /**
   * Build a map of key_hash => offset for the keys in the dirty portion of the log to use in cleaning.
   * @param log The log to use 要清理的log
   * @param start The offset at which dirty messages begin 脏数据的起始偏移量
   * @param end The ending offset for the map that is being built 脏数据的结束偏移量
   * @param map The map in which to store the mappings 用来存储消息键和offset的map
   *
   * @return The final offset the map covers
    * 真正清理日志
   */
  private[log] def buildOffsetMap(log: Log, start: Long, end: Long, map: OffsetMap): Long = {
    map.clear()
    // 获取从start到end所有的LogSegment
    val dirty = log.logSegments(start, end).toBuffer
    info("Building offset map for log %s for %d segments in offset range [%d, %d).".format(log.name, dirty.size, start, end))
    
    // Add all the dirty segments. We must take at least map.slots * load_factor,
    // but we may be able to fit more (if there is lots of duplication in the dirty section of the log)
    // 获取脏数据的起始偏移量
    var offset = dirty.head.baseOffset
    require(offset == start, "Last clean offset is %d but segment base offset is %d for log %s.".format(start, offset, log.name))
    var full = false
    // 遍历脏的segments
    for (segment <- dirty if !full) {
      // 检查topic-partiton的清理状态是否为LogCleaningAborted
      // 如果是则抛出异常
      checkDone(log.topicAndPartition)
      // 开始清理单个segment，将消息的key和offset添加到map中
      val newOffset = buildOffsetMapForSegment(log.topicAndPartition, segment, map)
      // 如果不为-1说明处理完了这个segment继续下一个
      if (newOffset > -1L)
        offset = newOffset
      // 为-1说明map满了
      else {
        // If not even one segment can fit in the map, compaction cannot happen
        require(offset > start, "Unable to build the offset map for segment %s/%s. You can increase log.cleaner.dedupe.buffer.size or decrease log.cleaner.threads".format(log.name, segment.log.file.getName))
        debug("Offset map is full, %d segments fully mapped, segment with base offset %d is partially mapped".format(dirty.indexOf(segment), segment.baseOffset))
        full = true
      }
    }
    info("Offset map for log %s complete.".format(log.name))
    // 返回已经读取到的偏移量
    offset
  }

  /**
   * Add the messages in the given segment to the offset map
   *
   * @param segment The segment to index
   * @param map The map in which to store the key=>offset mapping
   *
   * @return The final offset covered by the map or -1 if the map is full
    * 开始清理segment将对应的数据记录到给定的map中，如果map满了则返回-1
   */
  private def buildOffsetMapForSegment(topicAndPartition: TopicAndPartition, segment: LogSegment, map: OffsetMap): Long = {
    // 记录读取segment的位置
    var position = 0
    // segment的起始偏移量
    var offset = segment.baseOffset
    // map预期大小，达到该值在增加值后就会扩容
    val maxDesiredMapSize = (map.slots * this.dupBufferLoadFactor).toInt
    // 开始处理segment中的消息数据
    while (position < segment.log.sizeInBytes) {
      // 再次验证topic-partition状态
      checkDone(topicAndPartition)
      // 清理readBuffer，为接下来的读segment做准备
      readBuffer.clear()
      // 读取segment，从position位置开始读取
      val messages = new ByteBufferMessageSet(segment.log.readInto(readBuffer, position))
      // 限制速率？
      throttler.maybeThrottle(messages.sizeInBytes)
      // 记录本次读取的开始位置
      val startPosition = position
      // 遍历messages集合,获取一条条的消息以及消息偏移量
      for (entry <- messages) {
        val message = entry.message
        // 将message基于hasKey保存到map中，这样重复key的消息就会被较晚的消息覆盖
        if (message.hasKey) {
          // map没满就继续放
          if (map.size < maxDesiredMapSize)
            map.put(message.key, entry.offset)
          // map满了就返回-1，交给上层判断
          else {
            // The map is full, stop looping and return
            return -1L
          }
        }
        // 1.message没有key
        // 2.message有key && 已经放入了map中
        // 记录当前已处理消息的偏移量
        offset = entry.offset
        stats.indexMessagesRead(1)
      }
      // 获取当前segment已读取的字节数
      position += messages.validBytes
      stats.indexBytesRead(messages.validBytes)

      // if we didn't read even one complete message, our read buffer may be too small
      // 如果没有读取一条完整的消息，可能是读取缓冲区小了，扩容一下
      if(position == startPosition)
        growBuffers()
    }
    // 恢复readbuffer、writebuffer缓存区原始大小
    restoreBuffers()
    // 返回已处理到的offset
    offset
  }
}

/**
 * A simple struct for collecting stats about log cleaning
 */
private case class CleanerStats(time: Time = SystemTime) {
  var startTime, mapCompleteTime, endTime, bytesRead, bytesWritten, mapBytesRead, mapMessagesRead, messagesRead,
      messagesWritten, invalidMessagesRead = 0L
  var bufferUtilization = 0.0d
  clear()
  
  def readMessage(size: Int) {
    messagesRead += 1
    bytesRead += size
  }

  def invalidMessage() {
    invalidMessagesRead += 1
  }
  
  def recopyMessage(size: Int) {
    messagesWritten += 1
    bytesWritten += size
  }

  def indexMessagesRead(size: Int) {
    mapMessagesRead += size
  }

  def indexBytesRead(size: Int) {
    mapBytesRead += size
  }

  def indexDone() {
    mapCompleteTime = time.milliseconds
  }

  def allDone() {
    endTime = time.milliseconds
  }
  
  def elapsedSecs = (endTime - startTime)/1000.0
  
  def elapsedIndexSecs = (mapCompleteTime - startTime)/1000.0
  
  def clear() {
    startTime = time.milliseconds
    mapCompleteTime = -1L
    endTime = -1L
    bytesRead = 0L
    bytesWritten = 0L
    mapBytesRead = 0L
    mapMessagesRead = 0L
    messagesRead = 0L
    invalidMessagesRead = 0L
    messagesWritten = 0L
    bufferUtilization = 0.0d
  }
}

/**
 * Helper class for a log, its topic/partition, and the last clean position
  * @param topicPartition topic-partion
  * @param log 对应的Log
  * @param firstDirtyOffset 需要清理的起始位置
 */
private case class LogToClean(topicPartition: TopicAndPartition, log: Log, firstDirtyOffset: Long) extends Ordered[LogToClean] {
  // 已清理部分
  val cleanBytes = log.logSegments(-1, firstDirtyOffset).map(_.size).sum
  // 未清理部分
  val dirtyBytes = log.logSegments(firstDirtyOffset, math.max(firstDirtyOffset, log.activeSegment.baseOffset)).map(_.size).sum
  // 计算清理百分比，也就是需要清理数据的占比
  val cleanableRatio = dirtyBytes / totalBytes.toDouble
  // 总的字节数
  def totalBytes = cleanBytes + dirtyBytes
  override def compare(that: LogToClean): Int = math.signum(this.cleanableRatio - that.cleanableRatio).toInt
}
