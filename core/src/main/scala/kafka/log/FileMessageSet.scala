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
import java.nio._
import java.nio.channels._
import java.util.concurrent.atomic._

import kafka.utils._
import kafka.message._
import kafka.common.KafkaException
import java.util.concurrent.TimeUnit
import kafka.metrics.{KafkaTimer, KafkaMetricsGroup}
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.network.TransportLayer
import org.apache.kafka.common.utils.Utils

import scala.collection.mutable.ArrayBuffer

/**
 * An on-disk message set. An optional start and end position can be applied to the message set
  * 磁盘上的消息集合
 * which will allow slicing a subset of the file.
 * @param file The file name for the underlying log data
 * @param channel the underlying file channel used
 * @param start A lower bound on the absolute position in the file from which the message set begins
 * @param end The upper bound on the absolute position in the file at which the message set ends
 * @param isSlice Should the start and end parameters be used for slicing?
 */
@nonthreadsafe
class FileMessageSet private[kafka](@volatile var file: File,// 指向底层日志文件,如：xxxx.log
                                    private[log] val channel: FileChannel,// 对应底层日志文件的File的FileChannel，用于读写日志文件
                                    private[log] val start: Int,// FileMessageSet中存储消息的物理起始偏移量
                                    private[log] val end: Int,// FileMessageSet中存储消息的物理结束偏移量
                                    isSlice: Boolean) extends MessageSet with Logging {// 表示当前FileMessageSet是否为日志文件的分片

  /* the size of the message set in bytes */
  // FileMessageSet中存储的字节数
  private val _size =
    if(isSlice)
      new AtomicInteger(end - start) // don't check the file size if this is just a slice view
    else
      new AtomicInteger(math.min(channel.size.toInt, end) - start)

  /* if this is not a slice, update the file pointer to the end of the file */
  // 当前如果不是分片，那么将文件position指向文件末尾
  if (!isSlice)
    /* set the file position to the last byte in the file */
    channel.position(math.min(channel.size.toInt, end))

  /**
   * Create a file message set with no slicing.
   */
  def this(file: File, channel: FileChannel) =
    this(file, channel, start = 0, end = Int.MaxValue, isSlice = false)

  /**
   * Create a file message set with no slicing
   */
  def this(file: File) =
    this(file, FileMessageSet.openChannel(file, mutable = true))

  /**
   * Create a file message set with no slicing, and with initFileSize and preallocate.
   * For windows NTFS and some old LINUX file system, set preallocate to true and initFileSize
   * with one value (for example 512 * 1024 *1024 ) can improve the kafka produce performance.
   * If it's new file and preallocate is true, end will be set to 0.  Otherwise set to Int.MaxValue.
   */
  def this(file: File, fileAlreadyExists: Boolean, initFileSize: Int, preallocate: Boolean) =
      this(file,
        channel = FileMessageSet.openChannel(file, mutable = true, fileAlreadyExists, initFileSize, preallocate),
        start = 0,
        end = ( if ( !fileAlreadyExists && preallocate ) 0 else Int.MaxValue),
        isSlice = false)

  /**
   * Create a file message set with mutable option
   */
  def this(file: File, mutable: Boolean) = this(file, FileMessageSet.openChannel(file, mutable))

  /**
   * Create a slice view of the file message set that begins and ends at the given byte offsets
   */
  def this(file: File, channel: FileChannel, start: Int, end: Int) =
    this(file, channel, start, end, isSlice = true)

  /**
   * Return a message set which is a view into this set starting from the given position and with the given size limit.
   *
   * If the size is beyond the end of the file, the end will be based on the size of the file at the time of the read.
   *
   * If this message set is already sliced, the position will be taken relative to that slicing.
   *
   * @param position The start position to begin the read from
   * @param size The number of bytes after the start position to include
   *
   * @return A sliced wrapper on this message set limited based on the given position and size
    * 读取消息，这个重新创建了一个FileMessageSet对象，属于分片isSlice = true，后续读取消息可以通过channel以及start和end来读取
   */
  def read(position: Int, size: Int): FileMessageSet = {
    if(position < 0)
      throw new IllegalArgumentException("Invalid position: " + position)
    if(size < 0)
      throw new IllegalArgumentException("Invalid size: " + size)
    new FileMessageSet(file,
                       channel,
                       start = this.start + position,
                       end = math.min(this.start + position + size, sizeInBytes()))
  }

  /**
   * Search forward for the file position of the last offset that is greater than or equal to the target offset
   * and return its physical position. If no such offsets are found, return null.
   * @param targetOffset The offset to search for.
   * @param startingPosition The starting position in the file to begin searching from.
   */
  // 从物理位置startingPosition开始查找，当找到targetOffset逻辑位置时，返回

  // 这里就是从物理起始位置startingPosition开始查找目标逻辑结束位置targetOffset
  // 找到第一个逻辑位置offset >= 逻辑结束位置targetOffset的物理位置position，然后返回，返回内容就是
  // (物理结束位置position的逻辑位置offset，物理结束位置position)
  def searchFor(targetOffset: Long, startingPosition: Int): OffsetPosition = {
    // 记录要读取的物理起始位置
    var position = startingPosition
    // 创建一个12字节的buffer
    val buffer = ByteBuffer.allocate(MessageSet.LogOverhead)
    // 获取当前FileMessageSet存储消息的大小
    val size = sizeInBytes()
    // 从起始位置开始读取，如果 position + 消息LogOverhead >= size那就没消息可读了
    while(position + MessageSet.LogOverhead < size) {
      // 重置buffer的position指针，准备写内容到buffer
      buffer.rewind()
      // 读取消息12个字节的LogOverhead
      channel.read(buffer, position)
      // 没有读取到，报错
      if(buffer.hasRemaining)
        throw new IllegalStateException("Failed to read complete buffer for targetOffset %d startPosition %d in %s"
                                        .format(targetOffset, startingPosition, file.getAbsolutePath))
      // 重置buffer的position指针，准备读buffer
      buffer.rewind()
      // 获取消息的offset,8个字节
      val offset = buffer.getLong()
      // 如果读取的消息的逻辑偏移量offset >= targetOffset那么此时就返回这个最接近targetOffset的offset和对应的物理偏移量
      if(offset >= targetOffset)
        // 读取MessageSet的offset >= 读取位置,返回
        return OffsetPosition(offset, position)
      // 获取消息的size,4个字节
      val messageSize = buffer.getInt()
      // 消息有问题
      if(messageSize < Message.MinMessageOverhead)
        throw new IllegalStateException("Invalid message size: " + messageSize)
      // 获取下一个消息的物理偏移量
      position += MessageSet.LogOverhead + messageSize
    }
    null
  }

  /**
   * Write some of this set to the given channel.
   * @param destChannel The channel to write to. 接收数据的目标channel
   * @param writePosition The position in the message set to begin writing from. 写入的起始位置
   * @param size The maximum number of bytes to write 要写入消息的字节数
   * @return The number of bytes actually written. 实际写入的字节数
   */
  // 写入消息到destChannel
  def writeTo(destChannel: GatheringByteChannel, writePosition: Long, size: Int): Int = {
    // Ensure that the underlying size has not changed.
    // 确保文件大小没有改变，如果发生了改变那么一定是被truncate了
    val newSize = math.min(channel.size.toInt, end) - start
    if (newSize < _size.get()) {
      throw new KafkaException("Size of FileMessageSet %s has been truncated during write: old size %d, new size %d"
        .format(file.getAbsolutePath, _size.get(), newSize))
    }
    // 获取写入起始偏移量
    val position = start + writePosition
    // 取要写入目标channel的字节数和当前FileMessageSet存储的字节数中最小的那个
    val count = math.min(size, sizeInBytes)
    // 基于destChannel类型来调用不同的拷贝方法，将channel中的数据拷贝到destChannel中
    // 返回写入的字节数
    val bytesTransferred = (destChannel match {
        // 从channel的0开始读，读取count字节，然后从tl的position开始写入
      case tl: TransportLayer => tl.transferFrom(channel, position, count)
        // 从channel的position开始读，读取count字节，然后从dc的0开始写入
      case dc => channel.transferTo(position, count, dc)
    }).toInt
    trace("FileMessageSet " + file.getAbsolutePath + " : bytes transferred : " + bytesTransferred
      + " bytes requested for transfer : " + math.min(size, sizeInBytes))
    bytesTransferred
  }

  /**
    * This method is called before we write messages to the socket using zero-copy transfer. We need to
    * make sure all the messages in the message set have the expected magic value.
    *
    * @param expectedMagicValue the magic value expected 期待的魔法值
    * @return true if all messages have expected magic value, false otherwise
    */
  override def isMagicValueInAllWrapperMessages(expectedMagicValue: Byte): Boolean = {
    // 获取当前FileMessageSet消息的起始偏移量
    var location = start
    // 消息的头信息
    val offsetAndSizeBuffer = ByteBuffer.allocate(MessageSet.LogOverhead)
    // 消息的头信息，这里具体参考一条Message的结构
    // 4 byte CRC32 of the message
    // 1 byte "magic" identifier to allow format changes, value is 0 or 1
    // 1 byte "attributes" identifier to allow annotations on the message independent of the version
    // bit 0 ~ 2 : Compression codec.
    //   0 : no compression
    //   1 : gzip
    //   2 : snappy
    //   3 : lz4
    // bit 3 : Timestamp type
    //   0 : create time
    //   1 : log append time
    // bit 4 ~ 7 : reserved
    // (Optional) 8 byte timestamp only if "magic" identifier is greater than 0
    // 4 byte key length, containing length K
    // K byte key
    // 4 byte payload length, containing length V
    // V byte payload
    // 这里主要是定义一个Buffer，准备读取CRC32和magic
    val crcAndMagicByteBuffer = ByteBuffer.allocate(Message.CrcLength + Message.MagicLength)
    while (location < end) {
      // 读取消息的头信息做一些校验
      offsetAndSizeBuffer.rewind()
      channel.read(offsetAndSizeBuffer, location)
      // offsetAndSizeBuffer还有剩余空间，说明根本就没有消息，返回true就可以
      if (offsetAndSizeBuffer.hasRemaining)
        return true
      offsetAndSizeBuffer.rewind()
      // 先读取8个字节，跳过消息偏移量OffsetLength
      offsetAndSizeBuffer.getLong // skip offset field
      // 读取消息的字节数并验证
      val messageSize = offsetAndSizeBuffer.getInt
      if (messageSize < Message.MinMessageOverhead)
        throw new IllegalStateException("Invalid message size: " + messageSize)
      // 读取消息的CRC32和magic
      crcAndMagicByteBuffer.rewind()
      channel.read(crcAndMagicByteBuffer, location + MessageSet.LogOverhead)
      // 不等于期望值返回false
      if (crcAndMagicByteBuffer.get(Message.MagicOffset) != expectedMagicValue)
        return false
      // 继续处理下一条消息
      location += (MessageSet.LogOverhead + messageSize)
    }
    true
  }

  /**
   * Convert this message set to use the specified message format.
   */
  def toMessageFormat(toMagicValue: Byte): MessageSet = {
    val offsets = new ArrayBuffer[Long]
    val newMessages = new ArrayBuffer[Message]
    this.foreach { messageAndOffset =>
      val message = messageAndOffset.message
      if (message.compressionCodec == NoCompressionCodec) {
        newMessages += message.toFormatVersion(toMagicValue)
        offsets += messageAndOffset.offset
      } else {
        // File message set only has shallow iterator. We need to do deep iteration here if needed.
        val deepIter = ByteBufferMessageSet.deepIterator(messageAndOffset)
        for (innerMessageAndOffset <- deepIter) {
          newMessages += innerMessageAndOffset.message.toFormatVersion(toMagicValue)
          offsets += innerMessageAndOffset.offset
        }
      }
    }

    if (sizeInBytes > 0 && newMessages.size == 0) {
      // This indicates that the message is too large. We just return all the bytes in the file message set.
      this
    } else {
      // We use the offset seq to assign offsets so the offset of the messages does not change.
      new ByteBufferMessageSet(
        compressionCodec = this.headOption.map(_.message.compressionCodec).getOrElse(NoCompressionCodec),
        offsetSeq = offsets,
        newMessages: _*)
    }
  }

  /**
   * Get a shallow iterator over the messages in the set.
   */
  override def iterator: Iterator[MessageAndOffset] = iterator(Int.MaxValue)

  /**
   * Get an iterator over the messages in the set. We only do shallow iteration here.
   * @param maxMessageSize A limit on allowable message size to avoid allocating unbounded memory.
   * If we encounter a message larger than this we throw an InvalidMessageException.
   * @return The iterator.
   */
  def iterator(maxMessageSize: Int): Iterator[MessageAndOffset] = {
    new IteratorTemplate[MessageAndOffset] {
      var location = start
      val sizeOffsetLength = 12
      val sizeOffsetBuffer = ByteBuffer.allocate(sizeOffsetLength)

      override def makeNext(): MessageAndOffset = {
        if(location + sizeOffsetLength >= end)
          return allDone()

        // read the size of the item
        sizeOffsetBuffer.rewind()
        channel.read(sizeOffsetBuffer, location)
        if(sizeOffsetBuffer.hasRemaining)
          return allDone()

        sizeOffsetBuffer.rewind()
        val offset = sizeOffsetBuffer.getLong()
        val size = sizeOffsetBuffer.getInt()
        if(size < Message.MinMessageOverhead || location + sizeOffsetLength + size > end)
          return allDone()
        if(size > maxMessageSize)
          throw new CorruptRecordException("Message size exceeds the largest allowable message size (%d).".format(maxMessageSize))

        // read the item itself
        val buffer = ByteBuffer.allocate(size)
        channel.read(buffer, location + sizeOffsetLength)
        if(buffer.hasRemaining)
          return allDone()
        buffer.rewind()

        // increment the location and return the item
        location += size + sizeOffsetLength
        new MessageAndOffset(new Message(buffer), offset)
      }
    }
  }

  /**
   * The number of bytes taken up by this file set
   */
  def sizeInBytes(): Int = _size.get()

  /**
   * Append these messages to the message set
    * 添加消息到messageSet
   */
  def append(messages: ByteBufferMessageSet) {
    // 将消息写入channel
    val written = messages.writeFullyTo(channel)
    // 修改当前FileMessageSet大小
    _size.getAndAdd(written)
  }

  /**
   * Commit all written data to the physical disk
    * 将所有写数据提交到物理磁盘
   */
  def flush() = {
    channel.force(true)
  }

  /**
   * Close this message set
   */
  def close() {
    flush()
    trim()
    channel.close()
  }

  /**
   * Trim file when close or roll to next file
   */
  def trim() {
    truncateTo(sizeInBytes())
  }

  /**
   * Delete this message set from the filesystem
   * @return True iff this message set was deleted.
   */
  def delete(): Boolean = {
    CoreUtils.swallow(channel.close())
    file.delete()
  }

  /**
   * Truncate this file message set to the given size in bytes. Note that this API does no checking that the
   * given size falls on a valid message boundary.
   * In some versions of the JDK truncating to the same size as the file message set will cause an
   * update of the files mtime, so truncate is only performed if the targetSize is smaller than the
   * size of the underlying FileChannel.
   * It is expected that no other threads will do writes to the log when this function is called.
   * @param targetSize The size to truncate to. Must be between 0 and sizeInBytes.
   * @return The number of bytes truncated off
   */
  // 截断FileMessageSet到指定大小
  def truncateTo(targetSize: Int): Int = {
    // 当前FileMessageSet存储的字节数
    val originalSize = sizeInBytes
    if(targetSize > originalSize || targetSize < 0)
      throw new KafkaException("Attempt to truncate log segment to " + targetSize + " bytes failed, " +
                               " size of this log segment is " + originalSize + " bytes.")
    // 开始剪裁，就是修改channel
    if (targetSize < channel.size.toInt) {
      channel.truncate(targetSize)
      channel.position(targetSize)
      _size.set(targetSize)
    }
    // 返回截断的字节数
    originalSize - targetSize
  }

  /**
   * Read from the underlying file into the buffer starting at the given position
   */
  def readInto(buffer: ByteBuffer, relativePosition: Int): ByteBuffer = {
    channel.read(buffer, relativePosition + this.start)
    buffer.flip()
    buffer
  }

  /**
   * Rename the file that backs this message set
   * @throws IOException if rename fails.
   */
  def renameTo(f: File) {
    try Utils.atomicMoveWithFallback(file.toPath, f.toPath)
    finally this.file = f
  }

}

object FileMessageSet
{
  /**
   * Open a channel for the given file
   * For windows NTFS and some old LINUX file system, set preallocate to true and initFileSize
   * with one value (for example 512 * 1025 *1024 ) can improve the kafka produce performance.
    * 对于windows NTFS和一些旧的LINUX文件系统，
    * 设置preallocate为true和initFileSize一个值(例如512 * 1025 *1024)可以提高kafka产生的性能。
   * @param file File path
   * @param mutable mutable
   * @param fileAlreadyExists File already exists or not
   * @param initFileSize The size used for pre allocate file, for example 512 * 1025 *1024
   * @param preallocate Pre allocate file or not, gotten from configuration.
   */
  // 基于给定的文件创建一个FileChannel
  def openChannel(file: File, mutable: Boolean, fileAlreadyExists: Boolean = false, initFileSize: Int = 0, preallocate: Boolean = false): FileChannel = {
    // 文件是否随机访问
    if (mutable) {
      // 文件已经存在
      if (fileAlreadyExists)
        // 创建FileChannel
        new RandomAccessFile(file, "rw").getChannel()
       // 文件不存在
      else {
        // 是否为预分配
        if (preallocate) {
          // 预分配当前分解，文件大小为默认大小为512 * 1025 *1024,如果文件不存在会创建
          val randomAccessFile = new RandomAccessFile(file, "rw")
          randomAccessFile.setLength(initFileSize)
          randomAccessFile.getChannel()
        }
        else
          new RandomAccessFile(file, "rw").getChannel()
      }
    }
    else
      new FileInputStream(file).getChannel()
  }
}

object LogFlushStats extends KafkaMetricsGroup {
  val logFlushTimer = new KafkaTimer(newTimer("LogFlushRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
}
