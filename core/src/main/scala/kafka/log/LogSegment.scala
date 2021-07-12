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

import kafka.message._
import kafka.common._
import kafka.utils._
import kafka.server.{LogOffsetMetadata, FetchDataInfo}
import org.apache.kafka.common.errors.CorruptRecordException

import scala.math._
import java.io.{IOException, File}


 /**
 * A segment of the log. Each segment has two components: a log and an index. The log is a FileMessageSet containing
   * Log的一部分，每个LogSegment包括两部分内容：日志和索引，日志是一个FileMessageSet包含真实的消息
 * the actual messages. The index is an OffsetIndex that maps from logical offsets to physical file positions. Each
   * 索引是一个从逻辑偏移量映射到物理文件位置的OffsetIndex
 * segment has a base offset which is an offset <= the least offset of any message in this segment and > any offset in
   * 每一个LogSegment都有一个base offset，它小于这个LogSegment中所有消息的offset，但是 > 上一个LogSegment的所有offset
 * any previous segment.
 *
 * A segment with a base offset of [base_offset] would be stored in two files, a [base_offset].index and a [base_offset].log file.
 * LogSegment会基于自己的base offset日志和索引文件，base_offset.index和base_offset.log
 * @param log The message set containing log entries
 * @param index The offset index
 * @param baseOffset A lower bound on the offsets in this segment
 * @param indexIntervalBytes The approximate number of bytes between entries in the index
 * @param time The time instance
 */
@nonthreadsafe
class LogSegment(val log: FileMessageSet,// 存储消息集的FileMessageSet对象
                 val index: OffsetIndex,// 索引文件的OffsetIndex对象
                 val baseOffset: Long,// LogSegment第一个消息的offset
                 val indexIntervalBytes: Int,// 隔多少字节写一次索引 默认4096
                 val rollJitterMs: Long,
                 time: Time) extends Logging {

   // LogSegment创建时间
  var created = time.milliseconds

  /* the number of bytes since we last added an entry in the offset index */
   // 记录从上次在index文件中添加一个索引后，到现在为止日志文件中添加的字节数
   // 用来生成下一个索引
  private var bytesSinceLastIndexEntry = 0

   // 初始化LogSegment
  def this(dir: File, startOffset: Long, indexIntervalBytes: Int, maxIndexSize: Int, rollJitterMs: Long, time: Time, fileAlreadyExists: Boolean = false, initFileSize: Int = 0, preallocate: Boolean = false) =
    // 创建对应的FileMessageSet
    this(new FileMessageSet(file = Log.logFilename(dir, startOffset), fileAlreadyExists = fileAlreadyExists, initFileSize = initFileSize, preallocate = preallocate),
      // 创建索引文件
      new OffsetIndex(Log.indexFilename(dir, startOffset), baseOffset = startOffset, maxIndexSize = maxIndexSize),
         startOffset,
         indexIntervalBytes,
         rollJitterMs,
         time)

  /* Return the size in bytes of this log segment */
   // 返回该日志段目前消息集的大小(以字节为单位)
  def size: Long = log.sizeInBytes()

  /**
   * Append the given messages starting with the given offset. Add
   * an entry to the index if needed.
   *
   * It is assumed this method is being called from within a lock.
   *
   * @param offset The first offset in the message set.消息集的起始偏移量
   * @param messages The messages to append. 消息集
   */
  // 拼接给定的消息
  // producer一次性传过来的消息就是一个ByteBufferMessageSet，里面包含了客户端的多条消息
  @nonthreadsafe
  def append(offset: Long, messages: ByteBufferMessageSet) {
    if (messages.sizeInBytes > 0) {
      trace("Inserting %d bytes at offset %d at position %d".format(messages.sizeInBytes, offset, log.sizeInBytes()))
      // append an entry to the index (if needed)
      // 判断是否更新index索引
      if(bytesSinceLastIndexEntry > indexIntervalBytes) {
        // 添加索引，消息集第一个消息的offset，当前FileMessageSet已保存的消息字节数
        index.append(offset, log.sizeInBytes())
        // 重置累加消息字节数
        this.bytesSinceLastIndexEntry = 0
      }
      // 拼接消息集
      // append the messages
      log.append(messages)
      // 更新bytesSinceLastIndexEntry一遍判断后续是否需要写索引
      this.bytesSinceLastIndexEntry += messages.sizeInBytes
    }
  }

  /**
   * Find the physical file position for the first message with offset >= the requested offset.
   * The lowerBound argument is an optimization that can be used if we already know a valid starting position
   * in the file higher than the greatest-lower-bound from the index.
   *
   * @param offset The offset we want to translate
   * @param startingFilePosition A lower bound on the file position from which to begin the search. This is purely an optimization and
   * when omitted, the search will begin at the position in the offset index.
   *
   * @return The position in the log storing the message with the least offset >= the requested offset or null if no message meets this criteria.
   */
  // 转换一下
  @threadsafe
  private[log] def translateOffset(offset: Long, startingFilePosition: Int = 0): OffsetPosition = {
    // 基于消息的offset，查找小于或等于offset的物理位置
    val mapping: OffsetPosition = index.lookup(offset)
    // 从FileMessageSet中查找第一个大于或等于offset的位置
    log.searchFor(offset, max(mapping.position, startingFilePosition))
  }

  /**
   * Read a message set from this segment beginning with the first offset >= startOffset. The message set will include
   * no more than maxSize bytes and will end before maxOffset if a maxOffset is specified.
   *
   * @param startOffset A lower bound on the first offset to include in the message set we read 消息读取的起始offset
   * @param maxSize The maximum number of bytes to include in the message set we read 期望读取的最大字节数
   * @param maxOffset An optional maximum offset for the message set we read 消息可读取的最大offset(HW或者LEO)
   * @param maxPosition The maximum position in the log segment that should be exposed for read  允许读取的最大字节数(当前LogSegment中已写入消息的字节数)
   *
   * @return The fetched data and the offset metadata of the first message whose offset is >= startOffset,
   *         or null if the startOffset is larger than the largest offset in this log
   */
  @threadsafe
  def read(startOffset: Long, maxOffset: Option[Long], maxSize: Int, maxPosition: Long = size): FetchDataInfo = {
    if(maxSize < 0)
      throw new IllegalArgumentException("Invalid max size for log read (%d)".format(maxSize))
    // 重新获取当前LogSegment中已存储的消息字节数
    val logSize = log.sizeInBytes // this may change, need to save a consistent copy
    // 将读取消息的startOffset
    val startPosition:OffsetPosition = translateOffset(startOffset)

    // if the start position is already off the end of the log, return null
    // 没找到，返回null
    if(startPosition == null)
      return null
    // 封装一个LogOffsetMetadata(读取消息的起始offset,当前LogSegment的起始offset,以及读取消息的起始物理位置)
    val offsetMetadata = new LogOffsetMetadata(startOffset, this.baseOffset, startPosition.position)

    // if the size is zero, still return a log segment but with zero size
    // 如果读取字节数为0，一条消息不读，返回一个空
    if(maxSize == 0)
      return FetchDataInfo(offsetMetadata, MessageSet.Empty)

    // calculate the length of the message set to read based on whether or not they gave us a maxOffset
    // 重新计算可读取的字节数
    val length = maxOffset match {
        // maxOffset未指定
      case None =>
        // no max offset, just read until the max position
        // 允许读取的最大字节数位置 - startOffset对应的字节数位置 和 要读取消息的最大字节数，两种之间取最小
        min((maxPosition - startPosition.position).toInt, maxSize)
        // maxOffset被指定
      case Some(offset) =>
        // there is a max offset, translate it to a file position and use that to calculate the max read size;
        // when the leader of a partition changes, it's possible for the new leader's high watermark to be less than the
        // true high watermark in the previous leader for a short window. In this window, if a consumer fetches on an
        // offset between new leader's high watermark and the log end offset, we want to return an empty response.
        // 消息读取的起始offset > 消息可读取的最大offset
        if(offset < startOffset)
          return FetchDataInfo(offsetMetadata, MessageSet.Empty)
        // 计算消息可读取的最大字节数
        val mapping: OffsetPosition = translateOffset(offset, startPosition.position)
        val endPosition: Long =
          if(mapping == null)
            logSize // the max offset is off the end of the log, use the end of the file
          else
            mapping.position
        min(min(maxPosition, endPosition) - startPosition.position, maxSize).toInt
    }
    // 读取数据并返回，起始字节位置，以及长度
    FetchDataInfo(offsetMetadata, log.read(startPosition.position, length))
  }

  /**
   * Run recovery on the given segment. This will rebuild the index from the log file and lop off any invalid bytes from the end of the log and index.
   * 在给定的LogSegment上进行恢复。这将从日志文件重新构建索引，并删除日志和索引末尾的任何无效字节
   * @param maxMessageSize A bound the memory allocation in the case of a corrupt message size--we will assume any message larger than this
   * is corrupt. 超过参数maxMessageSize大小的消息都会丢弃掉
   *
   * @return The number of bytes truncated from the log
   */
  @nonthreadsafe
  def recover(maxMessageSize: Int): Int = {
    // 清空.index索引文件
    index.truncate()
    // 调整.index索引文件大小
    index.resize(index.maxIndexSize)
    // 已验证消息的字节数
    var validBytes = 0
    // 上一次写索引的物理偏移量
    var lastIndexEntry = 0
    // 构建一个迭代器
    val iter = log.iterator(maxMessageSize)
    try {
      // 迭代FileMessageSet中一个个消息集,重新构建索引
      // 当遍历的消息超过maxMessageSize大小后，会抛出一个CorruptRecordException异常
      while(iter.hasNext) {
        val entry: MessageAndOffset = iter.next
        // 根据消息内容计算出来的crc和消息中存储的crc做比较，是否相等，不等抛出InvalidMessageException异常
        entry.message.ensureValid()
        // 当验证的消息字节数超过indexIntervalBytes时写一次索引
        if(validBytes - lastIndexEntry > indexIntervalBytes) {
          // we need to decompress the message, if required, to get the offset of the first uncompressed message
          // 如果消息被压缩那么需要解压缩以便获取对应消息的起始逻辑偏移量
          val startOffset =
            entry.message.compressionCodec match {
              case NoCompressionCodec =>
                entry.offset
              case _ =>
                ByteBufferMessageSet.deepIterator(entry).next().offset
          }
          // 写索引，消息集第一条消息的offset，当前已验证消息的字节数
          index.append(startOffset, validBytes)
          // 更新lastIndexEntry
          lastIndexEntry = validBytes
        }
        // 累加已验证的消息字节数
        validBytes += MessageSet.entrySize(entry.message)
      }
    } catch {
      case e: CorruptRecordException =>
        logger.warn("Found invalid messages in log segment %s at byte offset %d: %s.".format(log.file.getAbsolutePath, validBytes, e.getMessage))
    }
    // 计算要截取FileMessageSet中的字节数
    val truncated = log.sizeInBytes - validBytes
    // 截取FileMessageSet，起始就是修改内容channel的position到validBytes
    log.truncateTo(validBytes)
    // 调整.index索引文件
    index.trimToValidSize()
    // 返回丢弃的字节数
    truncated
  }

  override def toString() = "LogSegment(baseOffset=" + baseOffset + ", size=" + size + ")"

  /**
   * Truncate off all index and log entries with offsets >= the given offset.
   * If the given offset is larger than the largest message in this segment, do nothing.
   * @param offset The offset to truncate to
   * @return The number of log bytes truncated
   */
  @nonthreadsafe
  def truncateTo(offset: Long): Int = {
    // 转移逻辑偏移量offset
    val mapping = translateOffset(offset)
    if(mapping == null)
      return 0
    // 截断索引
    index.truncateTo(offset)
    // after truncation, reset and allocate more space for the (new currently  active) index
    // 调整.index索引文件
    index.resize(index.maxIndexSize)
    // 截取FileMessageSet，这里获取offset对应的物理偏移量
    val bytesTruncated = log.truncateTo(mapping.position)
    // 截取FileMessageSet后没内容了，那么更新created
    if(log.sizeInBytes == 0)
      created = time.milliseconds
    // 重新计算索引累加值
    bytesSinceLastIndexEntry = 0
    bytesTruncated
  }

  /**
   * Calculate the offset that would be used for the next message to be append to this segment.
   * Note that this is expensive.
    * 计算LEO
   */
  @threadsafe
  def nextOffset(): Long = {
    val ms = read(index.lastOffset, None, log.sizeInBytes)
    if(ms == null) {
      baseOffset
    } else {
      ms.messageSet.lastOption match {
        case None => baseOffset
        case Some(last) => last.nextOffset
      }
    }
  }

  /**
   * Flush this log segment to disk
    * LogSegment刷磁盘
   */
  @threadsafe
  def flush() {
    LogFlushStats.logFlushTimer.time {
      log.flush()
      index.flush()
    }
  }

  /**
   * Change the suffix for the index and log file for this log segment
    * 修改.index和.log文件的后缀
   */
  def changeFileSuffixes(oldSuffix: String, newSuffix: String) {

    def kafkaStorageException(fileType: String, e: IOException) =
      new KafkaStorageException(s"Failed to change the $fileType file suffix from $oldSuffix to $newSuffix for log segment $baseOffset", e)

    try log.renameTo(new File(CoreUtils.replaceSuffix(log.file.getPath, oldSuffix, newSuffix)))
    catch {
      case e: IOException => throw kafkaStorageException("log", e)
    }
    try index.renameTo(new File(CoreUtils.replaceSuffix(index.file.getPath, oldSuffix, newSuffix)))
    catch {
      case e: IOException => throw kafkaStorageException("index", e)
    }
  }

  /**
   * Close this log segment
   */
  def close() {
    CoreUtils.swallow(index.close)
    CoreUtils.swallow(log.close)
  }

  /**
   * Delete this log segment from the filesystem.
   * @throws KafkaStorageException if the delete fails.
    * 从文件系统中删除此日志段
   */
  def delete() {
    // 删除LogSegment对应的底层.log和.index文件
    val deletedLog = log.delete()
    val deletedIndex = index.delete()
    // 校验是否删除成功
    if(!deletedLog && log.file.exists)
      throw new KafkaStorageException("Delete of log " + log.file.getName + " failed.")
    if(!deletedIndex && index.file.exists)
      throw new KafkaStorageException("Delete of index " + index.file.getName + " failed.")
  }

  /**
   * The last modified time of this log segment as a unix time stamp
    * 获取日志段对应的底层File文件最后的修改时间
   */
  def lastModified = log.file.lastModified

  /**
   * Change the last modified time for this log segment
    * 修改日志段对应的底层File文件最后的修改时间
   */
  def lastModified_=(ms: Long) = {
    log.file.setLastModified(ms)
    index.file.setLastModified(ms)
  }
}
