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

import kafka.utils._
import kafka.message._
import kafka.common._
import kafka.metrics.KafkaMetricsGroup
import kafka.server.{BrokerTopicStats, FetchDataInfo, LogOffsetMetadata}
import java.io.{File, IOException}
import java.lang
import java.util.concurrent.{ConcurrentNavigableMap, ConcurrentSkipListMap}
import java.util.concurrent.atomic._
import java.text.NumberFormat
import java.util.Map

import org.apache.kafka.common.errors.{CorruptRecordException, OffsetOutOfRangeException, RecordBatchTooLargeException, RecordTooLargeException}
import org.apache.kafka.common.record.TimestampType

import scala.collection.JavaConversions
import com.yammer.metrics.core.Gauge
import org.apache.kafka.common.utils.Utils

object LogAppendInfo {
  val UnknownLogAppendInfo = LogAppendInfo(-1, -1, Message.NoTimestamp, NoCompressionCodec, NoCompressionCodec, -1, -1, false)
}

/**
 * Struct to hold various quantities we compute about each message set before appending to the log
 * @param firstOffset The first offset in the message set
 * @param lastOffset The last offset in the message set
 * @param timestamp The log append time (if used) of the message set, otherwise Message.NoTimestamp
 * @param sourceCodec The source codec used in the message set (send by the producer)
 * @param targetCodec The target codec of the message set(after applying the broker compression configuration if any)
 * @param shallowCount The number of shallow messages
 * @param validBytes The number of valid bytes
 * @param offsetsMonotonic Are the offsets in this message set monotonically increasing
 */
case class LogAppendInfo(var firstOffset: Long,// 消息集起始offset
                         var lastOffset: Long,// 消息集结束offset
                         var timestamp: Long,// 时间戳
                         sourceCodec: CompressionCodec,// 生成端压缩格式
                         targetCodec: CompressionCodec,// 服务端压缩格式
                         shallowCount: Int,//  有效message个数
                         validBytes: Int,// 验证的字节数
                         offsetsMonotonic: Boolean)// 消息集中的偏移量是单调递增的吗


/**
 * An append-only log for storing messages.
 *
 * The log is a sequence of LogSegments, each with a base offset denoting the first message in the segment.
 *
 * New log segments are created according to a configurable policy that controls the size in bytes or time interval
 * for a given segment.
 *
 * @param dir The directory in which log segments are created.
 * @param config The log configuration settings
 * @param recoveryPoint The offset at which to begin recovery--i.e. the first offset which has not been flushed to disk
 * @param scheduler The thread pool scheduler used for background actions
 * @param time The time instance used for checking the clock
 *
 */
// 某个topic-partion对应的日志
@threadsafe
class Log(val dir: File,// 某个topic-partion对应的日志文件所在磁盘目录，如：/tmp/kafka-logs/simon-topic-0
          @volatile var config: LogConfig,// 日志配置设置
          @volatile var recoveryPoint: Long = 0L,// Log对应已刷入磁盘的偏移量，默认0，当存在Log日志时会被初始化为最后刷入磁盘的位置(参考：kafka.log.LogManager.loadLogs())
          scheduler: Scheduler,// kafkaServer初始时的KafkaScheduler
          time: Time = SystemTime) extends Logging with KafkaMetricsGroup { // 当前时间戳

  import kafka.log.Log._

  /* A lock that guards all modifications to the log */
  // 锁
  private val lock = new Object

  /* last time it was flushed */
  // 记录log目录上次flush的时间戳
  private val lastflushedTime = new AtomicLong(time.milliseconds)

  // 获取初始化一个LogSegment的大小
  def initFileSize() : Int = {
    if (config.preallocate)
      config.segmentSize
    else
      0
  }

  /* the actual segments of the log */
  // 记录所有的LogSegment,是一个跳表，key是LogSegment的baseOffset
  private val segments: ConcurrentNavigableMap[java.lang.Long, LogSegment] = new ConcurrentSkipListMap[java.lang.Long, LogSegment]
  // 恢复日志目录下的所有LogSegments
  loadSegments()

  /* Calculate the offset of the next message */
  // 记录了当前活跃LogSegment下一条消息的偏移量、当前活跃LogSegment起始位置、当前活跃LogSegment的相对物理位置
  @volatile var nextOffsetMetadata = new LogOffsetMetadata(activeSegment.nextOffset(), activeSegment.baseOffset, activeSegment.size.toInt)
  // 记录当前Log对应的主题和分区
  val topicAndPartition: TopicAndPartition = Log.parseTopicPartitionName(dir)

  info("Completed load of log %s with log end offset %d".format(name, logEndOffset))

  val tags = Map("topic" -> topicAndPartition.topic, "partition" -> topicAndPartition.partition.toString)

  newGauge("NumLogSegments",
    new Gauge[Int] {
      def value = numberOfSegments
    },
    tags)

  newGauge("LogStartOffset",
    new Gauge[Long] {
      def value = logStartOffset
    },
    tags)

  newGauge("LogEndOffset",
    new Gauge[Long] {
      def value = logEndOffset
    },
    tags)

  newGauge("Size",
    new Gauge[Long] {
      def value = size
    },
    tags)

  /** The name of this log */
  // 日志目录的名称，如/tmp/kafka-logs/simon-topic-0得到的就是simon-topic-0
  def name  = dir.getName()

  /* Load the log segments from the log files on disk */
  /**
    * 此方法只有在创建Log时会被调用,从磁盘加载LogSegments恢复日志
    */
  private def loadSegments() {
    // create the log directory if it doesn't exist
    // topic-partition对应的日志目录不存时则创建
    dir.mkdirs()
    // 记录".swap"结尾的日志文件
    var swapFiles = Set[File]()

    // first do a pass through the files in the log directory and remove any temporary files
    // and find any interrupted swap operations
    /**
      * 第一步: 遍历当前topic-partition的日志目录下的文件
      * 1. 将.deleted和.cleaned结尾的file删除
      * 2. 将.swap结尾的file的后缀.swap去掉
      *   2.1 去掉后缀的file以.index结尾,删除
      *   2.2 去掉后缀的file以.log结尾,查找对应的index文件(新建文件将.log替换为.index就是对应的索引文件),然后删除.
      *   2.3 最后将file文件记录到swapFiles集合中
      * 3. 上述并没有处理.log和.index结尾的日志文件
      */
    for(file <- dir.listFiles if file.isFile) {
      // 不可读抛出异常
      if(!file.canRead)
        throw new IOException("Could not read file " + file)
      // 获取文件名
      val filename = file.getName
      // 文件以.deleted、.cleaned结尾删除
      // .deleted结尾的文件为待删除的LogSegment,详见kafka.log.Log.asyncDeleteSegment()
      // .cleaned结尾的文件为在进行日志清理时创建的临时文件，详见kafka.log.Cleaner.cleanSegments()
      if(filename.endsWith(DeletedFileSuffix) || filename.endsWith(CleanedFileSuffix)) {
        // if the file ends in .deleted or .cleaned, delete it
        file.delete()
        // 文件以.swap结尾
      } else if(filename.endsWith(SwapFileSuffix)) {
        // we crashed in the middle of a swap operation, to recover:
        // if a log, delete the .index file, complete the swap operation later
        // if an index just delete it, it will be rebuilt
        // 将文件.swap的后缀去除
        val baseName = new File(CoreUtils.replaceSuffix(file.getPath, SwapFileSuffix, ""))
        // 如果是.index文件，那么删除
        if(baseName.getPath.endsWith(IndexFileSuffix)) {
          file.delete()
          // 如果是.log文件，删除其对应的.index文件，然后在将对应的.swap文件记录到swapFiles集合中
        } else if(baseName.getPath.endsWith(LogFileSuffix)){
          // delete the index
          val index = new File(CoreUtils.replaceSuffix(baseName.getPath, LogFileSuffix, IndexFileSuffix))
          index.delete()
          swapFiles += file
        }
      }
    }

    // now do a second pass and load all the .log and .index files
    /**
      * 第二步: 遍历当前topic-partition的日志目录下的文件,处理.log和.index结尾的日志文件
      * 1. file以.index结尾,获取对应的.log文件,判断.log文件不存在则删除.index文件
      * 2. file以.log结尾,获取file文件记录的消息起始偏移量start,
      *   2.1 基于start构建对应的indexFile
      *   2.2 基于start构建对应的LogSegment
      *   2.3 判断indexFile文件是否存在
      *     2.3.1 已存在,校验.index文件数据是否正确
      *       2.3.1.1 正确,不处理
      *       2.3.1.2 错误,删除.idnex索引文件,调用上面2.2创建的LogSegment的recover(config.maxMessageSize)恢复
      *     2.3.2 不存在,调用上面2.2创建的LogSegment的segment.recover(config.maxMessageSize)恢复
      * 3. 最后将start和LogSegment记录到segments集合中
      */
    for(file <- dir.listFiles if file.isFile) {
      val filename = file.getName
      // file以.index结尾
      if(filename.endsWith(IndexFileSuffix)) {
        // if it is an index file, make sure it has a corresponding .log file
        // 创建一个新文件,步骤就是将file文件的.index替换为.log，如果.log文件不存在那么删除.index文件
        val logFile = new File(file.getAbsolutePath.replace(IndexFileSuffix, LogFileSuffix))
        if(!logFile.exists) {
          warn("Found an orphaned index file, %s, with no corresponding log file.".format(file.getAbsolutePath))
          file.delete()
        }
        // file以.log结尾
      } else if(filename.endsWith(LogFileSuffix)) {
        // if its a log file, load the corresponding log segment
        // 将file文件的.log后缀去掉,就是当前log日志记录的消息起始偏移量
        val start = filename.substring(0, filename.length - LogFileSuffix.length).toLong
        // 基于当前log日志的起始偏移量start,创建file文件对应的.index索引文件
        val indexFile = Log.indexFilename(dir, start)
        // 生成对应的LogSegment，调用重载的构造方法
        val segment = new LogSegment(dir = dir,
                                     startOffset = start,
                                     indexIntervalBytes = config.indexInterval,// index.interval.bytes 4096
                                     maxIndexSize = config.maxIndexSize,// segment.index.bytes 10 * 1024 * 1024
                                     rollJitterMs = config.randomSegmentJitter,
                                     time = time,
                                     fileAlreadyExists = true)

        // 如果索引文件存在
        if(indexFile.exists()) {
          try {
            // 校验.index文件数据是否正确
              segment.index.sanityCheck()
          } catch {
            // 有问题
            case e: java.lang.IllegalArgumentException =>
              warn("Found a corrupted index file, %s, deleting and rebuilding index...".format(indexFile.getAbsolutePath))
              // 删除.index文件
              indexFile.delete()
              // 重建索引
              segment.recover(config.maxMessageSize)
          }
        }
        // 如果索引文件不存在
        else {
          error("Could not find index file corresponding to log file %s, rebuilding index...".format(segment.log.file.getAbsolutePath))
          // 重建索引
          segment.recover(config.maxMessageSize)
        }
        // 记录这个LogSegment到跳表segments中
        segments.put(start, segment)
      }
    }

    // Finally, complete any interrupted swap operations. To be crash-safe,
    // log files that are replaced by the swap segment should be renamed to .deleted
    // before the swap file is restored as the new segment file.
    /**
      * 第三步: 处理.swap文件
      */
    for (swapFile <- swapFiles) {
      // 去掉swapFile文件的.swap后缀
      val logFile = new File(CoreUtils.replaceSuffix(swapFile.getPath, SwapFileSuffix, ""))
      // 获取文件名
      val fileName = logFile.getName
      // 获取.log文件的起始偏移量
      val startOffset = fileName.substring(0, fileName.length - LogFileSuffix.length).toLong
      // 获取.log文件对应的.swap结尾的.index索引文件indexFile
      val indexFile = new File(CoreUtils.replaceSuffix(logFile.getPath, LogFileSuffix, IndexFileSuffix) + SwapFileSuffix)
      // 基于indexFile创建对应的OffsetIndex
      val index =  new OffsetIndex(indexFile, baseOffset = startOffset, maxIndexSize = config.maxIndexSize)
      // 生成新的LogSegment，调用默认的构造方法
      val swapSegment = new LogSegment(new FileMessageSet(file = swapFile),
                                       index = index,
                                       baseOffset = startOffset,
                                       indexIntervalBytes = config.indexInterval,
                                       rollJitterMs = config.randomSegmentJitter,
                                       time = time)
      info("Found log file %s from interrupted swap operation, repairing.".format(swapFile.getPath))
      // 重新构建LogSegment的索引文件
      swapSegment.recover(config.maxMessageSize)
      // 查找介于[baseOffset,nextOffset)之间的LogSegments
      val oldSegments = logSegments(swapSegment.baseOffset, swapSegment.nextOffset)
      // 替换LogSegments为新的LogSegment
      replaceSegments(swapSegment, oldSegments.toSeq, isRecoveredSwapFile = true)
    }

    // 如果跳表segments中没有LogSegment，说明上面三部曲鸡毛也没做.那么初始化一个LogSegment
    if(logSegments.size == 0) {
      // no existing segments, create a new mutable segment beginning at offset 0
      segments.put(0L, new LogSegment(dir = dir,
                                     startOffset = 0,
                                     indexIntervalBytes = config.indexInterval,
                                     maxIndexSize = config.maxIndexSize,
                                     rollJitterMs = config.randomSegmentJitter,
                                     time = time,
                                     fileAlreadyExists = false,
                                     initFileSize = this.initFileSize(),
                                     preallocate = config.preallocate))
    // 上面三部曲获取到了日志，那么kafkaServer可能是崩溃恢复，也可能是正常重启...
    } else {
      // 恢复当前Log，一个Log对应一个topic-partition
      recoverLog()
      // reset the index size of the currently active log segment to allow more entries
      // 重置当前活动日志段的索引大小，以允许更多的条目
      activeSegment.index.resize(config.maxIndexSize)
    }

  }

  // 更新nextOffsetMetadata也就是LEO
  private def updateLogEndOffset(messageOffset: Long) {
    nextOffsetMetadata = new LogOffsetMetadata(messageOffset, activeSegment.baseOffset, activeSegment.size.toInt)
  }

  /**
    * 恢复当前Log
    * .kafka_cleanshutdown文件在kafka.log.LogManager#shutdown()方法调用时生成
    * 还有Log在每次flush的时候虽然消息刷盘了但是recoveryPoint还没有刷盘，
    * 其需要等待checkpointRecoveryPointOffsets定时任务来执行或正常关闭...
    * 所以在异常关闭时会存在recoveryPoint < 刷盘消息offset的可能.这时就会出现unflushed的文件,这些文件需要单独恢复一些
    */
  private def recoverLog() {
    // if we have the clean shutdown marker, skip recovery
    // 如果存在.kafka_cleanshutdown文件表示正常关闭
    // 当前Log的recoveryPoint就是活跃LogSegment的LEO
    if(hasCleanShutdownFile) {
      this.recoveryPoint = activeSegment.nextOffset
      return
    }

    // okay we need to actually recovery this log
    // 异常关闭,需要借助recovery-point-offset-checkpoint文件来恢复Log
    // 从当前Log的recoveryPoint开始读取更大offset的LogSegments
    val unflushed: Iterator[LogSegment] = logSegments(this.recoveryPoint, Long.MaxValue).iterator
    while(unflushed.hasNext) {
      val curr: LogSegment = unflushed.next
      info("Recovering unflushed segment %d in log %s.".format(curr.baseOffset, name))
      val truncatedBytes =
        try {
          // 恢复索引文件
          curr.recover(config.maxMessageSize)
        } catch {
          case e: InvalidOffsetException =>
            val startOffset = curr.baseOffset
            warn("Found invalid offset during recovery for log " + dir.getName +". Deleting the corrupt segment and " +
                 "creating an empty one with starting offset " + startOffset)
            curr.truncateTo(startOffset)
        }
      // 如果存在截断操作，那么就删除对应的unflushed
      if(truncatedBytes > 0) {
        // we had an invalid message, delete all remaining log
        warn("Corruption found in segment %d of log %s, truncating to offset %d.".format(curr.baseOffset, name, curr.nextOffset))
        unflushed.foreach(deleteSegment)
      }
    }
  }

  /**
   * Check if we have the "clean shutdown" file
    * 如果数据目录下有.kafka_cleanshutdown文件就表示是正常关闭，就用不上恢复点了。
    * 如果上一次关闭是异常关闭的，kafka就会利用checkpoint来修复日志了
   */
  private def hasCleanShutdownFile() = new File(dir.getParentFile, CleanShutdownFile).exists()

  /**
   * The number of segments in the log.
   * Take care! this is an O(n) operation.
   */
  def numberOfSegments: Int = segments.size

  /**
   * Close this log
   */
  def close() {
    debug("Closing log " + name)
    lock synchronized {
      for(seg <- logSegments)
        seg.close()
    }
  }

  /**
   * Append this message set to the active segment of the log, rolling over to a fresh segment if necessary.
   *
   * This method will generally be responsible for assigning offsets to the messages,
   * however if the assignOffsets=false flag is passed we will only check that the existing offsets are valid.
   *
   * @param messages The message set to append
   * @param assignOffsets Should the log assign offsets to this message set or blindly apply what it is given
   *
   * @throws KafkaStorageException If the append fails due to an I/O error.
   *
   * @return Information about the appended messages including the first and last offset.
   */
  // 添加消息集合到日志有效的segment,如有必要，滚动到另一个段，主要负责给消息分配offsets，
  // 但是如果assignOffsets=false，我们只是检查存在的offsets是否有效
  def append(messages: ByteBufferMessageSet, assignOffsets: Boolean = true): LogAppendInfo = {
    // 验证消息集合中的消息
    // 生成一个LogAppendInfo(消息集合中第一条消息的offset, 消息集合中最后一条消息的offset，消息集合中消息压缩类型，服务端消息压缩类型，验证通过的消息总数，验证通过的消息总字节数，消息集合中消息是否单调递增)
    val appendInfo: LogAppendInfo = analyzeAndValidateMessageSet(messages)

    // if we have any valid messages, append them to the log
    // 如果没有有效的消息，退出
    if (appendInfo.shallowCount == 0)
      return appendInfo

    // trim any invalid bytes or partial messages before appending it to the on-disk log
    // 截取出消息集合messages中有效的消息
    var validMessages: ByteBufferMessageSet = trimInvalidBytes(messages, appendInfo)

    try {
      // they are valid, insert them in the log
      lock synchronized {
        // 判断是否需要为当前消息集分配offset，默认true
        if (assignOffsets) {
          // assign offsets to the message set
          // 先获取当前的nextOffsetMetadata
          val offset = new LongRef(nextOffsetMetadata.messageOffset)
          // 更新appendInfo中第一条消息的offset,不采用Producer分配的值
          appendInfo.firstOffset = offset.value
          val now = time.milliseconds
          // 再次验证并返回新的消息集合以及消息集合大小是否发生变更
          val (validatedMessages, messageSizesMaybeChanged) = try {
            // 验证并分配消息offset，内部会递增LongRef，返回一个元组
            // 此时并没有将 nextOffsetMetadata.messageOffset 逐步递增
            validMessages.validateMessagesAndAssignOffsets(offset,
                                                           now,
                                                           appendInfo.sourceCodec,
                                                           appendInfo.targetCodec,
                                                           config.compact,
                                                           config.messageFormatVersion.messageFormatVersion,
                                                           config.messageTimestampType,
                                                           config.messageTimestampDifferenceMaxMs)
          } catch {
            case e: IOException => throw new KafkaException("Error in validating messages while appending to log '%s'".format(name), e)
          }
          validMessages = validatedMessages
          // 由于上面一步操作调用了offsetCounter.getAndIncrement()方法
          // 所以这里更新appendInfo中lastOffset,也就是最后一条消息的offset,需要在-1
          appendInfo.lastOffset = offset.value - 1
          // 修改当前消息集appendInfo添加时间
          if (config.messageTimestampType == TimestampType.LOG_APPEND_TIME)
            appendInfo.timestamp = now

          // re-validate message sizes if there's a possibility that they have changed (due to re-compression or message
          // format conversion)
          // 如果消息大小有可能发生更改，则重新验证消息集合中每个消息大小是否超过允许的单个消息大小
          if (messageSizesMaybeChanged) {
            for (messageAndOffset <- validMessages.shallowIterator) {
              if (MessageSet.entrySize(messageAndOffset.message) > config.maxMessageSize) {
                // we record the original message set size instead of the trimmed size
                // to be consistent with pre-compression bytesRejectedRate recording
                BrokerTopicStats.getBrokerTopicStats(topicAndPartition.topic).bytesRejectedRate.mark(messages.sizeInBytes)
                BrokerTopicStats.getBrokerAllTopicsStats.bytesRejectedRate.mark(messages.sizeInBytes)
                throw new RecordTooLargeException("Message size is %d bytes which exceeds the maximum configured message size of %d."
                  .format(MessageSet.entrySize(messageAndOffset.message), config.maxMessageSize))
              }
            }
          }
        // 用producer端给定的offset
        } else {
          // we are taking the offsets we are given
          // 如果消息集中消息的offset非递增 或者 消息集合中第一条消息的offset < LEO那么抛出异常
          // 所以这里可以看出如果Producer分配offset,需要递增并且要从≥ nextOffsetMetadata.messageOffset位置开始分配
          if (!appendInfo.offsetsMonotonic || appendInfo.firstOffset < nextOffsetMetadata.messageOffset)
            throw new IllegalArgumentException("Out of order offsets found in " + messages)
        }

        // check messages set size may be exceed config.segmentSize
        // 验证消息大小是否超过segmentSize(默认1 * 1024 * 1024 * 1024=1G)
        if (validMessages.sizeInBytes > config.segmentSize) {
          throw new RecordBatchTooLargeException("Message set size is %d bytes which exceeds the maximum configured segment size of %d."
            .format(validMessages.sizeInBytes, config.segmentSize))
        }

        // maybe roll the log if this segment is full
        // 判断当前的LogSegment是否无法放下当前的消息集，如果是则新建一个LogSegment
        val segment = maybeRoll(validMessages.sizeInBytes)

        // now append to the log
        // 将消息添加到LogSegment
        segment.append(appendInfo.firstOffset, validMessages)

        // increment the log end offset
        // 重新生成LogOffsetMetadata
        updateLogEndOffset(appendInfo.lastOffset + 1)

        trace("Appended message set to log %s with first offset: %d, next offset: %d, and messages: %s"
          .format(this.name, appendInfo.firstOffset, nextOffsetMetadata.messageOffset, validMessages))
        // 触发flush阈值flush.messages，默认Long.MaxValue
        if (unflushedMessages >= config.flushInterval)
          flush()

        appendInfo
      }
    } catch {
      case e: IOException => throw new KafkaStorageException("I/O exception in append to log '%s'".format(name), e)
    }
  }

  /**
   * Validate the following:
   * <ol>
   * <li> each message matches its CRC
   * <li> each message size is valid
   * </ol>
   *
   * Also compute the following quantities:
   * <ol>
   * <li> First offset in the message set
   * <li> Last offset in the message set
   * <li> Number of messages
   * <li> Number of valid bytes
   * <li> Whether the offsets are monotonically increasing
   * <li> Whether any compression codec is used (if many are used, then the last one is given)
   * </ol>
    * 验证并解析消息集合然后返回一个LogAppendInfo
    * 封装了ByteBufferMessageSet 第一个offset,最后一个offset，生产者采用的压缩方式，追加到log的时间戳，
    * 服务端采用的压缩方式，外层消息个数，通过验证的总字节数
   */
  private def analyzeAndValidateMessageSet(messages: ByteBufferMessageSet): LogAppendInfo = {
    // 记录外层消息数量
    var shallowMessageCount = 0
    // 记录通过验证的消息的字节数和
    var validBytesCount = 0
    // 记录第一条消息和最后一条消息
    var firstOffset, lastOffset = -1L
    // producer的消息压缩方式
    var sourceCodec: CompressionCodec = NoCompressionCodec
    // 消息集合是否单独递增
    var monotonic = true
    // 遍历消息集合进行检查
    for(messageAndOffset <- messages.shallowIterator) {
      // update the first offset if on the first message
      // 更新第一条消息的offset，此时的offset还是生产者分配的offset
      // 参考org.apache.kafka.clients.producer.internals.RecordBatch#tryAppend
      if(firstOffset < 0)
        firstOffset = messageAndOffset.offset
      // check that offsets are monotonically increasing
      // 判断内部offset是否单调递增
      if(lastOffset >= messageAndOffset.offset)
        monotonic = false
      // update the last offset seen
      // 更新最后一条消息的offset
      lastOffset = messageAndOffset.offset
      // 获取消息
      val m = messageAndOffset.message

      // Check if the message sizes are valid.
      // 检测消息的大小
      // 获取消息的大小 = MessageSizeLength(4) + OffsetLength(8) + message.size
      val messageSize = MessageSet.entrySize(m)
      // 如果消息的大小 > max.message.bytes(默认：1000000 + MessageSizeLength + OffsetLength)抛出异常
      if(messageSize > config.maxMessageSize) {
        BrokerTopicStats.getBrokerTopicStats(topicAndPartition.topic).bytesRejectedRate.mark(messages.sizeInBytes)
        BrokerTopicStats.getBrokerAllTopicsStats.bytesRejectedRate.mark(messages.sizeInBytes)
        throw new RecordTooLargeException("Message size is %d bytes which exceeds the maximum configured message size of %d."
          .format(messageSize, config.maxMessageSize))
      }

      // check the validity of the message by checking CRC
      // 通过检查CRC来验证消息的有效性
      m.ensureValid()

      // 通过则增加外层消息数
      shallowMessageCount += 1
      // 通过则增加外层消息字节数
      validBytesCount += messageSize

      // 记录生产者采用的压缩方式
      val messageCodec = m.compressionCodec
      if(messageCodec != NoCompressionCodec)
        sourceCodec = messageCodec
    }

    // Apply broker-side compression if any
    // 获取服务端采用的压缩方式
    val targetCodec = BrokerCompressionCodec.getTargetCompressionCodec(config.compressionType, sourceCodec)
    // 生成一个LogAppendInfo(消息集合中第一条消息的offset, 消息集合中最后一条消息的offset，消息集合中消息压缩类型，服务端消息压缩类型，验证通过的消息总数，验证通过的消息总字节数，消息集合中消息是否单调递增)
    LogAppendInfo(firstOffset, lastOffset, Message.NoTimestamp, sourceCodec, targetCodec, shallowMessageCount, validBytesCount, monotonic)
  }

  /**
   * Trim any invalid bytes from the end of this message set (if there are any)
    * 修剪此消息集末尾的任何无效字节(如果有的话)
   * @param messages The message set to trim 消息集合
   * @param info The general information of the message set
   * @return A trimmed message set. This may be the same as what was passed in or it may not.
   */
  private def trimInvalidBytes(messages: ByteBufferMessageSet, info: LogAppendInfo): ByteBufferMessageSet = {
    // 获取消息集合中通过验证的字节数
    val messageSetValidBytes = info.validBytes
    if(messageSetValidBytes < 0)
      throw new CorruptRecordException("Illegal length of message set " + messageSetValidBytes + " Message set cannot be appended to log. Possible causes are corrupted produce requests")
    // 通过验证的字节数 == 消息集合大小，返回消息集合即可
    if(messageSetValidBytes == messages.sizeInBytes) {
      messages
    // 不相等，截取消息集合messages
    } else {
      // trim invalid bytes
      // 截取就是通过修改ByteBuffer的limit，截止到上面计算出的已通过的消息字节数，最后返回一个新的ByteBufferMessageSet
      val validByteBuffer = messages.buffer.duplicate()
      validByteBuffer.limit(messageSetValidBytes)
      new ByteBufferMessageSet(validByteBuffer)
    }
  }

  /**
   * Read messages from the log.
   *
   * @param startOffset The offset to begin reading at 起始offset
   * @param maxLength The maximum number of bytes to read 读取最大字节数
   * @param maxOffset The offset to read up to, exclusive. (i.e. this offset NOT included in the resulting message set) 读取最大offset,可能不存在
   *
   * @throws OffsetOutOfRangeException If startOffset is beyond the log end offset or before the base offset of the first segment.
   * @return The fetch data information including fetch starting offset metadata and messages read.
   */
  // 读取消息
  def read(startOffset: Long, maxLength: Int, maxOffset: Option[Long] = None): FetchDataInfo = {
    trace("Reading %d bytes from offset %d in log %s of length %d bytes".format(maxLength, startOffset, name, size))

    // Because we don't use lock for reading, the synchronization is a little bit tricky.
    // We create the local variables to avoid race conditions with updates to the log.
    // 获取该topic-partition当前activeSegment的LEO,baseOffset以及已写入字节数
    val currentNextOffsetMetadata: LogOffsetMetadata = nextOffsetMetadata
    val next: Long = currentNextOffsetMetadata.messageOffset
    // 读取的offset == LEO,无消息可读,返回空消息集和nextOffsetMetadata信息
    if(startOffset == next)
      return FetchDataInfo(currentNextOffsetMetadata, MessageSet.Empty)

    // 返回≤startOffset(最接近)的Entry对象,没有则返回null
    var entry: Map.Entry[lang.Long, LogSegment] = segments.floorEntry(startOffset)

    // attempt to read beyond the log end offset is an error
    // 1.startOffset > LEO
    // 2.Entry对象为null(说明startOffset查找的消息已经被过期定时任务删除了)
    // 抛出OffsetOutOfRangeException，如果是follower拉取消息会针对这种情况再处理 参考：kafka.server.ReplicaFetcherThread.handleOffsetOutOfRange
    if(startOffset > next || entry == null)
      throw new OffsetOutOfRangeException("Request for offset %d but we only have log segments in the range %d to %d.".format(startOffset, segments.firstKey, next))

    // Do the read on the segment with a base offset less than the target offset
    // but if that segment doesn't contain any messages with an offset greater than that
    // continue to read from successive segments until we get some messages or we reach the end of the log
    while(entry != null) {
      // If the fetch occurs on the active segment, there might be a race condition where two fetch requests occur after
      // the message is appended but before the nextOffsetMetadata is updated. In that case the second fetch may
      // cause OffsetOutOfRangeException. To solve that, we cap the reading up to exposed position instead of the log
      // end of the active segment.
      // 如果fetch发生在活动段上，可能会有一个竞争条件，即两个fetch请求发生在消息追加之后，但在nextffsetmetadata更新之前。
      // 在这种情况下，第二次取回可能会导致OffsetOutOfRangeException异常。为了解决这个问题，我们将读取限制在暴露的位置，而不是活动段的日志端。

      // 计算当前可读取的最大字节数
      val maxPosition: Long = {
        // entry是lastEntry(active segment),此时可能lastEntry正在不断被写入数据
        if (entry == segments.lastEntry) {
          // 防止OffsetOutOfRangeException，先获取当前lastEntry已写入的字节数
          val exposedPos = nextOffsetMetadata.relativePositionInSegment.toLong
          // Check the segment again in case a new segment has just rolled out.
          // 再次判断，如果lastEntry已经发生变更,重新获取entry已写入的字节数
          if (entry != segments.lastEntry)
            // New log segment has rolled out, we can read up to the file end.
            entry.getValue.size
          else
            exposedPos
        // 如果≤startOffset(最接近)的entry对象不是lastEntry,那么直接获取该LogSegment中的字节数接口
        } else {
          entry.getValue.size
        }
      }
      // 获取entry中的LogSegment,读取里面的消息(起始offset,最大offset,期望读取的最大字节数,允许读取的最大字节数)
      val fetchInfo: FetchDataInfo = entry.getValue.read(startOffset, maxOffset, maxLength, maxPosition)
      // 消息为空查找entry中LogSegment的下一个LogSegment
      if(fetchInfo == null) {
        entry = segments.higherEntry(entry.getKey)
      } else {
        return fetchInfo
      }
    }

    // okay we are beyond the end of the last segment with no data fetched although the start offset is in range,
    // 虽然起始偏移量在范围内，但我们已经超出了最后一个段的末尾，没有获取任何数据
    // this can happen when all messages with offset larger than start offsets have been deleted.
    // 当删除了所有偏移量大于start偏移量的消息时，就会发生这种情况。
    // In this case, we will return the empty set with log end offset metadata
    // 在本例中，我们将返回带有LEO偏移量元数据的空集
    // 返回一个空的FetchDataInfo，
    FetchDataInfo(nextOffsetMetadata, MessageSet.Empty)
  }

  /**
   * Given a message offset, find its corresponding offset metadata in the log.
   * If the message offset is out of range, return unknown offset metadata
   */
  def convertToOffsetMetadata(offset: Long): LogOffsetMetadata = {
    try {
      val fetchDataInfo = read(offset, 1)
      fetchDataInfo.fetchOffsetMetadata
    } catch {
      case e: OffsetOutOfRangeException => LogOffsetMetadata.UnknownOffsetMetadata
    }
  }

  /**
   * Delete any log segments matching the given predicate function,
   * starting with the oldest segment and moving forward until a segment doesn't match.
   * @param predicate A function that takes in a single log segment and returns true iff it is deletable
   * @return The number of segments deleted
    * 删除日志
   */
  def deleteOldSegments(predicate: LogSegment => Boolean): Int = {
    lock synchronized {
      //find any segments that match the user-supplied predicate UNLESS it is the final segment
      //and it is empty (since we would just end up re-creating it)
      // 上来就是获取segments列表中的最后一个Entry
      // 注意：这里获取的是Entry，而不是value
      val lastEntry = segments.lastEntry
      // 通过predicate来判断是否需要删除
      // deletable记录了所有需要删除的LogSegment
      val deletable: Iterable[LogSegment] =
        // lastEntry为null，segments为空没日志
        if (lastEntry == null) Seq.empty
        // 获取segments的value也就是所有当前Log下的所有LogSegment，从尾部循环遍历
        // 如果遍历出的LogSegment符合predicate && s不是最后一个lastEntry那么就将s加入到deletable中暂存
        // 那么问题来了，什么时候执行到【s.size > 0】?
        // 看代码一定是前面的false了，也就是s是最后一个LogSegment并且符合被删除的条件，那么才会删除，否则删除LogSegment的
        // 时候都是从倒数第二个开始
        else logSegments.takeWhile(s => predicate(s) && (s.baseOffset != lastEntry.getValue.baseOffset || s.size > 0))
      val numToDelete = deletable.size
      // 有符合条件需要删除的LogSegment
      if (numToDelete > 0) {
        // we must always have at least one segment, so if we are going to delete all the segments, create a new one first
        // 如果删除了所有的segment，那么需要在新建立一个LogSegment
        // 要不日志就没地方写了
        if (segments.size == numToDelete)
          // 新建一个LogSegment
          roll()
        // remove the segments for lookups
        // 循环删除LogSegment
        deletable.foreach(deleteSegment(_))
      }
      numToDelete
    }
  }

  /**
   * The size of the log in bytes
   */
  def size: Long = logSegments.map(_.size).sum

   /**
   * The earliest message offset in the log
   */
  def logStartOffset: Long = logSegments.head.baseOffset

  /**
   * The offset metadata of the next message that will be appended to the log
   */
  def logEndOffsetMetadata: LogOffsetMetadata = nextOffsetMetadata

  /**
   *  The offset of the next message that will be appended to the log
    *  将追加到日志中的下一条消息的偏移量
   */
  def logEndOffset: Long = nextOffsetMetadata.messageOffset

  /**
   * Roll the log over to a new empty log segment if necessary.
   *
   * @param messagesSize The messages set size in bytes 消息集合保存消息的字节数
   * logSegment will be rolled if one of the following conditions met
   * <ol>
   * <li> The logSegment is full
   * <li> The maxTime has elapsed
   * <li> The index is full
   * </ol>
   * @return The currently active segment after (perhaps) rolling to a new segment
   */
  private def maybeRoll(messagesSize: Int): LogSegment = {
    // 获取当前Log中活跃的日志段activeSegment
    val segment = activeSegment

    if (segment.size > config.segmentSize - messagesSize || // 当前活跃segment空间不够
        // 这里看一下创建LogSegment还多了一个扰动值，当前LogSegment已写入消息并且从创建到现在过去的时间已经超过了config.segmentMs - segment.rollJitterMs那么也需要创建新的LogSegment
       // 也就是说LogSegment创建包含两中：大小维度，默认1G，时间维度
        segment.size > 0 && time.milliseconds - segment.created > config.segmentMs - segment.rollJitterMs ||
        segment.index.isFull) { // 索引文件满了
      debug("Rolling new log segment in %s (log_size = %d/%d, index_size = %d/%d, age_ms = %d/%d)."
            .format(name,
                    segment.size,
                    config.segmentSize,
                    segment.index.entries,
                    segment.index.maxEntries,
                    time.milliseconds - segment.created,
                    config.segmentMs - segment.rollJitterMs))
      // 新建segment
      roll()
    // 否则返回当前活跃的日志段即可
    } else {
      segment
    }
  }

  /**
   * Roll the log over to a new active segment starting with the current logEndOffset.
   * This will trim the index to the exact size of the number of entries it currently contains.
   * @return The newly rolled segment
   */
  def roll(): LogSegment = {
    val start = time.nanoseconds
    lock synchronized {
      val newOffset = logEndOffset
      // 创建log文件，文件名是newOffset
      val logFile = logFilename(dir, newOffset)
      // 创建index文件，文件名也是newOffset
      val indexFile = indexFilename(dir, newOffset)
      // 如果已经存则删除
      for(file <- List(logFile, indexFile); if file.exists) {
        warn("Newly rolled segment file " + file.getName + " already exists; deleting it first")
        file.delete()
      }
      // 判断当前segments最后一个LogSegment是否存在
      // 不存在啥也不干，如果存在那么需要将最后一个LogSegment"封版"了，^_^
      segments.lastEntry() match {
        case null =>
        case entry => {
          entry.getValue.index.trimToValidSize()
          entry.getValue.log.trim()
        }
      }
      // 创建LogSegment
      val segment = new LogSegment(dir,
                                   startOffset = newOffset,
                                   indexIntervalBytes = config.indexInterval,
                                   maxIndexSize = config.maxIndexSize,
                                   rollJitterMs = config.randomSegmentJitter,
                                   time = time,
                                   fileAlreadyExists = false,
                                   initFileSize = initFileSize,
                                   preallocate = config.preallocate)
      // 记录segment到segments中
      val prev = addSegment(segment)
      // 不为null，说明已经存在
      if(prev != null)
        throw new KafkaException("Trying to roll a new log segment for topic partition %s with start offset %d while it already exists.".format(name, newOffset))
      // We need to update the segment base offset and append position data of the metadata when log rolls.
      // The next offset should not change.
      // 由于新创建了segment，所以更新nextOffsetMetadata
      // nextOffsetMetadata记录了当前活跃LogSegment的信息
      updateLogEndOffset(nextOffsetMetadata.messageOffset)
      // schedule an asynchronous flush of the old segment
      // 为newOffset之前的数据执行flus操作
      scheduler.schedule("flush-log", () => flush(newOffset), delay = 0L)

      info("Rolled new log segment for '" + name + "' in %.0f ms.".format((System.nanoTime - start) / (1000.0*1000.0)))
      // 返回新建的LogSegment
      segment
    }
  }

  /**
   * The number of messages appended to the log since the last flush
   */
  def unflushedMessages() = this.logEndOffset - this.recoveryPoint

  /**
   * Flush all log segments
    * flush所有的segment，注意在执行flus前LEO已经更新
   */
  def flush(): Unit = flush(this.logEndOffset)

  /**
   * Flush log segments for all offsets up to offset-1
    * 刷新到offset-1的所有偏移的日志段
   * @param offset The offset to flush up to (non-inclusive); the new recovery point
    * 由于每次刷磁盘时刷入的数据都是小于offset的并且刷磁盘后更新recoveryPoint为offset,所以每次
    * 刷磁盘时都只需查找[recoveryPoint,offset)之间的数据即可
   */
  def flush(offset: Long) : Unit = {
    // offset之前的数据已经全部刷到磁盘，所以不需要刷新
    if (offset <= this.recoveryPoint)
      return
    debug("Flushing log '" + name + " up to offset " + offset + ", last flushed: " + lastFlushTime + " current time: " +
          time.milliseconds + " unflushed = " + unflushedMessages)
    // segments是跳表，查找recoveryPoint和offset(LEO并且不包含offset)之间的LogSegment对象
    for(segment <- logSegments(this.recoveryPoint, offset))
      // 调用操作系统fsync命令刷新到磁盘
      segment.flush()
    lock synchronized {
      // 如果 offset > this.recoveryPoint
      if(offset > this.recoveryPoint) {
        // 更新recoveryPoint
        this.recoveryPoint = offset
        // 更新lastflushedTime时间戳
        lastflushedTime.set(time.milliseconds)
      }
    }
  }

  /**
   * Completely delete this log directory and all contents from the file system with no delay
   */
  private[log] def delete() {
    lock synchronized {
      removeLogMetrics()
      logSegments.foreach(_.delete())
      segments.clear()
      Utils.delete(dir)
    }
  }

  /**
   * Truncate this log so that it ends with the greatest offset < targetOffset.
   * @param targetOffset The offset to truncate to, an upper bound on all offsets in the log after truncation is complete.
    * 截断日志，使其偏移量 < targetOffset
   */
  private[log] def truncateTo(targetOffset: Long) {
    info("Truncating log %s to offset %d.".format(name, targetOffset))
    if(targetOffset < 0)
      throw new IllegalArgumentException("Cannot truncate to a negative offset (%d).".format(targetOffset))
    if(targetOffset > logEndOffset) {
      info("Truncating %s to %d has no effect as the largest offset in the log is %d.".format(name, targetOffset, logEndOffset-1))
      return
    }
    lock synchronized {
      // 如果其实segment的起始offset > targetOffset
      // 那么这说明那就是清空所有的日志
      if(segments.firstEntry.getValue.baseOffset > targetOffset) {
        truncateFullyAndStartAt(targetOffset)
      } else {
        // 获取所有的LogSegment，然后将baseOffset > targetOffset的Segment清理掉
        val deletable = logSegments.filter(segment => segment.baseOffset > targetOffset)
        // 删除需要清理的Segment
        deletable.foreach(deleteSegment(_))
        // 将活跃Segment截取到targetOffset
        activeSegment.truncateTo(targetOffset)
        // 更新当前LEO为targetOffset
        updateLogEndOffset(targetOffset)
        // 更新当前Log对应最新的recoveryPoint
        this.recoveryPoint = math.min(targetOffset, this.recoveryPoint)
      }
    }
  }

  /**
   *  Delete all data in the log and start at the new offset
   *  @param newOffset The new offset to start the log with
   */
  private[log] def truncateFullyAndStartAt(newOffset: Long) {
    debug("Truncate and start log '" + name + "' to " + newOffset)
    lock synchronized {
      val segmentsToDelete = logSegments.toList
      segmentsToDelete.foreach(deleteSegment(_))
      addSegment(new LogSegment(dir,
                                newOffset,
                                indexIntervalBytes = config.indexInterval,
                                maxIndexSize = config.maxIndexSize,
                                rollJitterMs = config.randomSegmentJitter,
                                time = time,
                                fileAlreadyExists = false,
                                initFileSize = initFileSize,
                                preallocate = config.preallocate))
      updateLogEndOffset(newOffset)
      this.recoveryPoint = math.min(newOffset, this.recoveryPoint)
    }
  }

  /**
   * The time this log is last known to have been fully flushed to disk
   */
  // 最后一次执行flush操作的时间戳
  def lastFlushTime(): Long = lastflushedTime.get

  /**
   * The active segment that is currently taking appends
   */
  // 当前活跃的segment
  def activeSegment = segments.lastEntry.getValue

  /**
   * All the log segments in this log ordered from oldest to newest
    * 获取当前topic-partition对应的Log下的所有日志段
   */
  def logSegments: Iterable[LogSegment] = {
    import JavaConversions._
    segments.values
  }

  /**
   * Get all segments beginning with the segment that includes "from" and ending with the segment
   * that includes up to "to-1" or the end of the log (if to > logEndOffset)
    * 从segments中查找LogSegment，处于[from,to)之后，
    * 如果to > LEO那么就是[from,to]
   */
  def logSegments(from: Long, to: Long): Iterable[LogSegment] = {
    import JavaConversions._
    lock synchronized {
      // 返回小于等于from的最大key；如果不存在返回null
      val floor = segments.floorKey(from)
      if(floor eq null)
        // 返回小于to的所有entry
        segments.headMap(to).values
      else
       // 返回所有介于[from,to)之间的entry
        segments.subMap(floor, true, to, false).values
    }
  }

  override def toString() = "Log(" + dir + ")"

  /**
   * This method performs an asynchronous log segment delete by doing the following:
   * <ol>
   *   <li>It removes the segment from the segment map so that it will no longer be used for reads.
   *   <li>It renames the index and log files by appending .deleted to the respective file name
   *   <li>It schedules an asynchronous delete operation to occur in the future
   * </ol>
   * This allows reads to happen concurrently without synchronization and without the possibility of physically
   * deleting a file while it is being read from.
   *
   * @param segment The log segment to schedule for deletion
    * 删除LogSegment
   */
  private def deleteSegment(segment: LogSegment) {
    info("Scheduling log segment %d for log %s for deletion.".format(segment.baseOffset, name))
    lock synchronized {
      // 先从segments中剔除
      segments.remove(segment.baseOffset)
      // 异步删除
      asyncDeleteSegment(segment)
    }
  }

  /**
   * Perform an asynchronous delete on the given file if it exists (otherwise do nothing)
   * @throws KafkaStorageException if the file can't be renamed and still exists
    *  异步删除segment
   */
  private def asyncDeleteSegment(segment: LogSegment) {
    // 将segment的文件加上后缀.deleted
    segment.changeFileSuffixes("", Log.DeletedFileSuffix)
    // 对应segment定义一个删除方法
    def deleteSeg() {
      info("Deleting segment %d from log %s.".format(segment.baseOffset, name))
      segment.delete()
    }
    // 将删除方法教给定时任务来执行，file.delete.delay.ms默认60000执行一次
    scheduler.schedule("delete-file", deleteSeg, delay = config.fileDeleteDelayMs)
  }

  /**
   * Swap a new segment in place and delete one or more existing segments in a crash-safe manner. The old segments will
   * be asynchronously deleted.
   *
   * The sequence of operations is:
   * <ol>
   *   <li> Cleaner creates new segment with suffix .cleaned and invokes replaceSegments().
   *        If broker crashes at this point, the clean-and-swap operation is aborted and
   *        the .cleaned file is deleted on recovery in loadSegments().
   *   <li> New segment is renamed .swap. If the broker crashes after this point before the whole
   *        operation is completed, the swap operation is resumed on recovery as described in the next step.
   *   <li> Old segment files are renamed to .deleted and asynchronous delete is scheduled.
   *        If the broker crashes, any .deleted files left behind are deleted on recovery in loadSegments().
   *        replaceSegments() is then invoked to complete the swap with newSegment recreated from
   *        the .swap file and oldSegments containing segments which were not renamed before the crash.
   *   <li> Swap segment is renamed to replace the existing segment, completing this operation.
   *        If the broker crashes, any .deleted files which may be left behind are deleted
   *        on recovery in loadSegments().
   * </ol>
   *
   * @param newSegment The new log segment to add to the log 要添加到日志中的新日志段
   * @param oldSegments The old log segments to delete from the log 将旧的日志段从日志中删除
   * @param isRecoveredSwapFile true if the new segment was created from a swap file during recovery after a crash
    *                            如果新段是在崩溃后的恢复期间从交换文件创建的，则为true
   */
  private[log] def replaceSegments(newSegment: LogSegment, oldSegments: Seq[LogSegment], isRecoveredSwapFile : Boolean = false) {
    lock synchronized {
      // need to do this in two phases to be crash safe AND do the delete asynchronously
      // if we crash in the middle of this we complete the swap in loadSegments()
      if (!isRecoveredSwapFile)
        newSegment.changeFileSuffixes(Log.CleanedFileSuffix, Log.SwapFileSuffix)
      // 将新LogSegment加入到segments跳表
      addSegment(newSegment)

      // delete the old files
      // 删除旧的LogSegment
      for(seg <- oldSegments) {
        // remove the index entry
        // 先从segments跳表删除
        if(seg.baseOffset != newSegment.baseOffset)
          segments.remove(seg.baseOffset)
        // delete segment
        // 异步任务删除物理文件
        asyncDeleteSegment(seg)
      }
      // okay we are safe now, remove the swap suffix
      // 将新LogSegment的.swap后缀去掉
      newSegment.changeFileSuffixes(Log.SwapFileSuffix, "")
    }
  }

  /**
   * remove deleted log metrics
   */
  private[log] def removeLogMetrics(): Unit = {
    removeMetric("NumLogSegments", tags)
    removeMetric("LogStartOffset", tags)
    removeMetric("LogEndOffset", tags)
    removeMetric("Size", tags)
  }
  /**
   * Add the given segment to the segments in this log. If this segment replaces an existing segment, delete it.
   * @param segment The segment to add
   */
  def addSegment(segment: LogSegment) = this.segments.put(segment.baseOffset, segment)

}

/**
 * Helper functions for logs
  * 日志的辅助函数,一些文件后缀
 */
object Log {

  /** a log file */
  val LogFileSuffix = ".log"

  /** an index file */
  val IndexFileSuffix = ".index"

  /**
    * a file that is scheduled to be deleted 计划要删除的文件
    * 参考 kafka.log.Log.asyncDeleteSegment
    */
  val DeletedFileSuffix = ".deleted"

  /** A temporary file that is being used for log cleaning 用于清理日志的临时文件*/
  val CleanedFileSuffix = ".cleaned"

  /** A temporary file used when swapping files into the log 在将文件交换到日志中时使用的临时文件*/
  val SwapFileSuffix = ".swap"

  /** Clean shutdown file that indicates the broker was cleanly shutdown in 0.8. This is required to maintain backwards compatibility
    * with 0.8 and avoid unnecessary log recovery when upgrading from 0.8 to 0.8.1 */
  /** TODO: Get rid of CleanShutdownFile in 0.8.2 */
  val CleanShutdownFile = ".kafka_cleanshutdown"

  /**
    * 根据传入的offset构造一个20位的字符串,不够补0
    * 调用示例: val str = filenamePrefixFromOffset(109857)
    * 返回结果: 00000000000000109857
    * Make log segment file name from offset bytes. All this does is pad out the offset number with zeros
    * so that ls sorts the files numerically.
    * @param offset The offset to use in the file name
   * @return The filename
   */
  def filenamePrefixFromOffset(offset: Long): String = {
    val nf = NumberFormat.getInstance()
    nf.setMinimumIntegerDigits(20)
    nf.setMaximumFractionDigits(0)
    nf.setGroupingUsed(false)
    nf.format(offset)
  }

  /**
   * Construct a log file name in the given dir with the given base offset
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   */
  def logFilename(dir: File, offset: Long) =
    new File(dir, filenamePrefixFromOffset(offset) + LogFileSuffix)

  /**
    * 使用给定的offset在给定的目录dir中构造一个索引文件名
   * Construct an index file name in the given dir using the given base offset
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   */
  def indexFilename(dir: File, offset: Long) =
    new File(dir, filenamePrefixFromOffset(offset) + IndexFileSuffix)


  /**
   * Parse the topic and partition out of the directory name of a log
    * 从某个topic-partiton的日志目录名解析主题和分区,
    * 日志目录名结构:test-topic-0,解析结果topic: test-topic, partition: 0
   */
  def parseTopicPartitionName(dir: File): TopicAndPartition = {
    val name: String = dir.getName
    if (name == null || name.isEmpty || !name.contains('-')) {
      throwException(dir)
    }
    val index = name.lastIndexOf('-')
    val topic: String = name.substring(0, index)
    val partition: String = name.substring(index + 1)
    if (topic.length < 1 || partition.length < 1) {
      throwException(dir)
    }
    TopicAndPartition(topic, partition.toInt)
  }

  def throwException(dir: File) {
    throw new KafkaException("Found directory " + dir.getCanonicalPath + ", " +
      "'" + dir.getName + "' is not in the form of topic-partition\n" +
      "If a directory does not contain Kafka topic data it should not exist in Kafka's log " +
      "directory")
  }
}

