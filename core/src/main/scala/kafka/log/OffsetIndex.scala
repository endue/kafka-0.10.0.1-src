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

import org.apache.kafka.common.utils.Utils

import scala.math._
import java.io._
import java.nio._
import java.nio.channels._
import java.util.concurrent.locks._
import kafka.utils._
import kafka.utils.CoreUtils.inLock
import kafka.common.InvalidOffsetException

/**
 * An index that maps offsets to physical file locations for a particular log segment. This index may be sparse:
 * that is it may not hold an entry for all messages in the log.
 * 
 * The index is stored in a file that is pre-allocated to hold a fixed maximum number of 8-byte entries.
 * 
 * The index supports lookups against a memory-map of this file. These lookups are done using a simple binary search variant
 * to locate the offset/location pair for the greatest offset less than or equal to the target offset.
 * 
 * Index files can be opened in two ways: either as an empty, mutable index that allows appends or
 * an immutable read-only index file that has previously been populated. The makeReadOnly method will turn a mutable file into an 
 * immutable one and truncate off any extra bytes. This is done when the index file is rolled over.
 * 
 * No attempt is made to checksum the contents of this file, in the event of a crash it is rebuilt.
 * 
 * The file format is a series of entries. The physical format is a 4 byte "relative" offset and a 4 byte file location for the 
 * message with that offset. The offset stored is relative to the base offset of the index file. So, for example,
 * if the base offset was 50, then the offset 55 would be stored as 5. Using relative offsets in this way let's us use
 * only 4 bytes for the offset.
 * 
 * The frequency of entries is up to the user of this class.
 * 
 * All external APIs translate from relative offsets to full offsets, so users of this class do not interact with the internal 
 * storage format.
 */
/**
  * offset索引
  * @param _file 指向索引文件
  * @param baseOffset 对应日志文件第一个消息的offset
  * @param maxIndexSize
  */
class OffsetIndex(@volatile private[this] var _file: File, val baseOffset: Long, val maxIndexSize: Int = -1) extends Logging {
  
  private val lock = new ReentrantLock
  
  /* initialize the memory mapping for this index */
  // 初始化索引文件的内存映射
  @volatile
  private[this] var mmap: MappedByteBuffer = {
    // 索引文件不存在就创建并返回true，否则返回false
    val newlyCreated = _file.createNewFile()
    val raf = new RandomAccessFile(_file, "rw")
    try {
      /* pre-allocate the file if necessary */
      // 如果需要，预分配文件
      if (newlyCreated) {
        if (maxIndexSize < 8)
          throw new IllegalArgumentException("Invalid max index size: " + maxIndexSize)
        // 找到一个比maxIndexSize数字小并且最接近8的倍数的数字
        raf.setLength(roundToExactMultiple(maxIndexSize, 8))
      }

      /* memory-map the file */
      val len = raf.length()
      // 映射底层.index文件
      val idx = raf.getChannel.map(FileChannel.MapMode.READ_WRITE, 0, len)

      /* set the position in the index for the next entry */
      // 如果是新索引文件，从头开始写入数据
      if (newlyCreated)
        idx.position(0)
      else
        // if this is a pre-existing index, assume it is all valid and set position to last entry
        // 索引文件已存在，将position移动到索引项的结束位置，防止数据覆盖
        idx.position(roundToExactMultiple(idx.limit, 8))
      idx
    } finally {
      CoreUtils.swallow(raf.close())
    }
  }

  /* the number of eight-byte entries currently in the index */
  // 一个索引占8个字节，这里计算索引文件已存储的索引数量
  @volatile
  private[this] var _entries = mmap.position / 8

  /* The maximum number of eight-byte entries this index can hold */
  // 一个索引占8个字节，这里计算索引文件可存储的索引数量
  @volatile
  private[this] var _maxEntries = mmap.limit / 8

  // 最后一条索引记录的逻辑偏移量offset
  @volatile
  private[this] var _lastOffset = readLastEntry.offset
  
  debug("Loaded index file %s with maxEntries = %d, maxIndexSize = %d, entries = %d, lastOffset = %d, file position = %d"
    .format(_file.getAbsolutePath, _maxEntries, maxIndexSize, _entries, _lastOffset, mmap.position))

  /** The maximum number of entries this index can hold */
  // 可存储的索引数量
  def maxEntries: Int = _maxEntries

  /** The last offset in the index */
  // 最后一条索引的逻辑偏移量
  def lastOffset: Long = _lastOffset

  /** The index file */
  // 索引文件
  def file: File = _file

  /**
   * The last entry in the index
    * 读取最后一条索引记录的偏移量
   */
  def readLastEntry(): OffsetPosition = {
    inLock(lock) {
      _entries match {
         // 没有存储索引，返回的就是当前索引文件的baseOffset，物理偏移量为0
        case 0 => OffsetPosition(baseOffset, 0)
        // 有存储索引，返回的就是最后一条索引的逻辑偏移量和物理偏移量
        case s => OffsetPosition(baseOffset + relativeOffset(mmap, s - 1), physical(mmap, s - 1))
      }
    }
  }

  /**
   * Find the largest offset less than or equal to the given targetOffset 
   * and return a pair holding this offset and its corresponding physical file position.
   * 
   * @param targetOffset The offset to look up.
   * 
   * @return The offset found and the corresponding file position for this offset. 
   * If the target offset is smaller than the least entry in the index (or the index is empty),
   * the pair (baseOffset, 0) is returned.
    * 查找
    * targetOffset为消息的逻辑偏移量offset
   */
  def lookup(targetOffset: Long): OffsetPosition = {
    maybeLock(lock) {
      // 创建索引文件快照
      val idx = mmap.duplicate
      // 二分查找逻辑偏移量targetOffset在索引文件中属于第几个条目
      val slot = indexSlotFor(idx, targetOffset)
      if(slot == -1)
        OffsetPosition(baseOffset, 0)
      else
        // 返回targetOffset对应消息的逻辑偏移量offset和物理偏移量
        OffsetPosition(baseOffset + relativeOffset(idx, slot), physical(idx, slot))
      }
  }
  
  /**
   * Find the slot in which the largest offset less than or equal to the given
   * target offset is stored.
   * 
   * @param idx The index buffer
   * @param targetOffset The offset to look for
   * 
   * @return The slot found or -1 if the least entry in the index is larger than the target offset or the index is empty
   */
  // 基于索引文件来查找目标偏移量
  private def indexSlotFor(idx: ByteBuffer, targetOffset: Long): Int = {
    // we only store the difference from the base offset so calculate that
    // 计算targetOffset相对索引文件baseOffset的实际偏移量
    val relOffset = targetOffset - baseOffset
    
    // check if the index is empty
    // 索引文件没内容，直接返回-1，没找到
    if (_entries == 0)
      return -1
    
    // check if the target offset is smaller than the least offset
    // 如果要查找的实际偏移量比当前索引的baseOffset还小，那也是不需要查找了，直接返回-1
    if (relativeOffset(idx, 0) > relOffset)
      return -1
      
    // binary search for the entry
    // 二分查找
    var lo = 0// 起始
    var hi = _entries - 1// 最大(从0开始的，所以这里-1)
    while (lo < hi) {
      val mid = ceil(hi/2.0 + lo/2.0).toInt
      val found = relativeOffset(idx, mid)
      if (found == relOffset)
        return mid
      else if (found < relOffset)
        lo = mid
      else
        hi = mid - 1
    }
    lo
  }
  
  /* return the nth offset relative to the base offset */
  // 返回相对于base offset的第n个位置的逻辑偏移量offset
  private def relativeOffset(buffer: ByteBuffer, n: Int): Int = buffer.getInt(n * 8)
  
  /* return the nth physical position */
  // 返回第n个位置的物理偏移量
  private def physical(buffer: ByteBuffer, n: Int): Int = buffer.getInt(n * 8 + 4)
  
  /**
   * Get the nth offset mapping from the index
   * @param n The entry number in the index
   * @return The offset/position pair at that entry
   */
  def entry(n: Int): OffsetPosition = {
    maybeLock(lock) {
      if(n >= _entries)
        throw new IllegalArgumentException("Attempt to fetch the %dth entry from an index of size %d.".format(n, _entries))
      val idx = mmap.duplicate
      OffsetPosition(relativeOffset(idx, n), physical(idx, n))
    }
  }
  
  /**
   * Append an entry for the given offset/location pair to the index. This entry must have a larger offset than all subsequent entries.
    * 添加索引
    * offset：为消息集中第一个消息的offset
    * position：为保存消息集FileMessageSet当前的大小(传递进来时还未保存消息集)
   */
  def append(offset: Long, position: Int) {
    inLock(lock) {
      require(!isFull, "Attempt to append to a full index (size = " + _entries + ").")
      if (_entries == 0 || offset > _lastOffset) {
        debug("Adding index entry %d => %d to %s.".format(offset, position, _file.getName))
        // 写入4个字节的基于baseOffset的偏移量(逻辑偏移量)
        mmap.putInt((offset - baseOffset).toInt)
        // 写入4个字节的消息集大小(物理偏移量)
        mmap.putInt(position)
        _entries += 1
        // 更新_lastOffset为最新加入消息的offset
        _lastOffset = offset
        require(_entries * 8 == mmap.position, _entries + " entries but file position in index is " + mmap.position + ".")
      } else {
        throw new InvalidOffsetException("Attempt to append an offset (%d) to position %d no larger than the last offset appended (%d) to %s."
          .format(offset, _entries, _lastOffset, _file.getAbsolutePath))
      }
    }
  }
  
  /**
   * True iff there are no more slots available in this index
   */
  def isFull: Boolean = _entries >= _maxEntries
  
  /**
   * Truncate the entire index, deleting all entries
    * 截断整个索引，删除所有条目
   */
  def truncate() = truncateToEntries(0)
  
  /**
   * Remove all entries from the index which have an offset greater than or equal to the given offset.
    * 从索引中删除所有偏移量大于或等于给定偏移量的项
   * Truncating to an offset larger than the largest in the index has no effect.
   */
  def truncateTo(offset: Long) {
    inLock(lock) {
      val idx = mmap.duplicate
      val slot = indexSlotFor(idx, offset)

      /* There are 3 cases for choosing the new size
       * 1) if there is no entry in the index <= the offset, delete everything
       * 2) if there is an entry for this exact offset, delete it and everything larger than it
       * 3) if there is no entry for this offset, delete everything larger than the next smallest
       */
      val newEntries = 
        if(slot < 0)
          0
        else if(relativeOffset(idx, slot) == offset - baseOffset)
          slot
        else
          slot + 1
      truncateToEntries(newEntries)
    }
  }

  /**
   * Truncates index to a known number of entries.
    * 截取索引为指定的条数
   */
  private def truncateToEntries(entries: Int) {
    inLock(lock) {
      // 更新目前记录的索引条数
      _entries = entries
      // 更新文件的写入文章
      mmap.position(_entries * 8)
      // 更新最后一条索引的逻辑偏移量
      _lastOffset = readLastEntry.offset
    }
  }
  
  /**
   * Trim this segment to fit just the valid entries, deleting all trailing unwritten bytes from
   * the file.
   */
  def trimToValidSize() {
    inLock(lock) {
      resize(_entries * 8)
    }
  }

  /**
   * Reset the size of the memory map and the underneath file. This is used in two kinds of cases: (1) in
   * trimToValidSize() which is called at closing the segment or new segment being rolled; (2) at
   * loading segments from disk or truncating back to an old segment where a new log segment became active;
   * we want to reset the index size to maximum index size to avoid rolling new segment.
    * 重置索引大小
   */
  def resize(newSize: Int) {
    inLock(lock) {
      val raf = new RandomAccessFile(_file, "rw")
      // 基于newSize查找到一个最接近8的倍数的roundedNewSize
      val roundedNewSize = roundToExactMultiple(newSize, 8)
      // mmap对应buffer的position
      val position = mmap.position
      
      /* Windows won't let us modify the file length while the file is mmapped :-( */
      if (Os.isWindows)
        // 强制释放MappedByteBuffer
        forceUnmap(mmap)
      try {
        // 重新生成mmap并设置缓存区大小为roundedNewSize
        raf.setLength(roundedNewSize)
        mmap = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, roundedNewSize)
        _maxEntries = mmap.limit / 8
        // 设置mmap写入的起始位置
        mmap.position(position)
      } finally {
        CoreUtils.swallow(raf.close())
      }
    }
  }
  
  /**
   * Forcefully free the buffer's mmap. We do this only on windows.
   */
  private def forceUnmap(m: MappedByteBuffer) {
    try {
      if(m.isInstanceOf[sun.nio.ch.DirectBuffer])
        (m.asInstanceOf[sun.nio.ch.DirectBuffer]).cleaner().clean()
    } catch {
      case t: Throwable => warn("Error when freeing index buffer", t)
    }
  }
  
  /**
   * Flush the data in the index to disk
   */
  def flush() {
    inLock(lock) {
      mmap.force()
    }
  }
  
  /**
   * Delete this index file
   */
  def delete(): Boolean = {
    info("Deleting index " + _file.getAbsolutePath)
    if (Os.isWindows)
      CoreUtils.swallow(forceUnmap(mmap))
    _file.delete()
  }
  
  /** The number of entries in this index */
  def entries = _entries
  
  /**
   * The number of bytes actually used by this index
   */
  def sizeInBytes() = 8 * _entries
  
  /** Close the index */
  def close() {
    trimToValidSize()
  }
  
  /**
   * Rename the file that backs this offset index
   * @throws IOException if rename fails
   */
  def renameTo(f: File) {
    try Utils.atomicMoveWithFallback(_file.toPath, f.toPath)
    finally _file = f
  }
  
  /**
   * Do a basic sanity check on this index to detect obvious problems
   * @throws IllegalArgumentException if any problems are found
   */
  def sanityCheck() {
    require(_entries == 0 || lastOffset > baseOffset,
            "Corrupt index found, index file (%s) has non-zero size but the last offset is %d and the base offset is %d"
            .format(_file.getAbsolutePath, lastOffset, baseOffset))
    val len = _file.length()
    require(len % 8 == 0,
            "Index file " + _file.getName + " is corrupt, found " + len +
            " bytes which is not positive or not a multiple of 8.")
  }
  
  /**
   * Round a number to the greatest exact multiple of the given factor less than the given number.
   * E.g. roundToExactMultiple(67, 8) == 64
    * 找到一个比number数字小并且最接近factor倍数的数字
   */
  private def roundToExactMultiple(number: Int, factor: Int) = factor * (number / factor)
  
  /**
   * Execute the given function in a lock only if we are running on windows. We do this 
   * because Windows won't let us resize a file while it is mmapped. As a result we have to force unmap it
   * and this requires synchronizing reads.
   */
  private def maybeLock[T](lock: Lock)(fun: => T): T = {
    if(Os.isWindows)
      lock.lock()
    try {
      fun
    } finally {
      if(Os.isWindows)
        lock.unlock()
    }
  }
}
