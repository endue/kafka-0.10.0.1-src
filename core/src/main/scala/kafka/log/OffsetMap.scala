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

import java.util.Arrays
import java.security.MessageDigest
import java.nio.ByteBuffer
import kafka.utils._
import org.apache.kafka.common.utils.Utils

trait OffsetMap {
  def slots: Int
  def put(key: ByteBuffer, offset: Long)
  def get(key: ByteBuffer): Long
  def clear()
  def size: Int
  def utilization: Double = size.toDouble / slots
}

/**
 * An hash table used for deduplicating the log. This hash table uses a cryptographicly secure hash of the key as a proxy for the key
 * for comparisons and to save space on object overhead. Collisions are resolved by probing. This hash table does not support deletes.
 * @param memory The amount of memory this map can use 这个映射可以使用的内存量
 * @param hashAlgorithm The hash algorithm instance to use: MD2, MD5, SHA-1, SHA-256, SHA-384, SHA-512 要使用的哈希算法实例
 */
@nonthreadsafe
class SkimpyOffsetMap(val memory: Int, val hashAlgorithm: String = "MD5") extends OffsetMap {
  // 字节空间
  private val bytes = ByteBuffer.allocate(memory)
  
  /* the hash algorithm instance to use, default is MD5 */
  // 算法
  private val digest = MessageDigest.getInstance(hashAlgorithm)
  
  /* the number of bytes for this hash algorithm */
  // 通过算法进行信息摘要后的字节长度
  private val hashSize = digest.getDigestLength
  
  /* create some hash buffers to avoid reallocating each time */
  private val hash1 = new Array[Byte](hashSize)
  private val hash2 = new Array[Byte](hashSize)
  
  /* number of entries put into the map */
  private var entries = 0
  
  /* number of lookups on the map */
  private var lookups = 0L
  
  /* the number of probes for all lookups */
  private var probes = 0L
  
  /**
   * The number of bytes of space each entry uses (the number of bytes in the hash plus an 8 byte offset)
   */
  // 每个entry使用的空间字节数(散列中的字节数加上8字节的偏移量)
  val bytesPerEntry = hashSize + 8
  
  /**
   * The maximum number of entries this map can contain
    * 当前map可保存的entry数
   */
  val slots: Int = memory / bytesPerEntry
  
  /**
   * Associate this offset to the given key.
   * @param key The key
   * @param offset The offset
    * 往map中放入数据
   */
  override def put(key: ByteBuffer, offset: Long) {
    require(entries < slots, "Attempt to add a new entry to a full offset map.")
    lookups += 1
    // 对key进行加密然后保存到hash1中
    hashInto(key, hash1)
    // probe until we find the first empty slot
    // 探测找到第一个空槽
    var attempt = 0
    // 返回hash1要放入的槽
    var pos = positionOf(hash1, attempt)
    // 如果槽pos位置不为空
    while(!isEmpty(pos)) {
      // 读取pos位置的数据到hash2
      bytes.position(pos)
      bytes.get(hash2)
      // 旧值和新值相等，覆盖原值key的offset
      if(Arrays.equals(hash1, hash2)) {
        // we found an existing entry, overwrite it and return (size does not change)
        bytes.putLong(offset)
        return
      }
      // 旧值和新值不相等，查找下一个槽
      attempt += 1
      pos = positionOf(hash1, attempt)
    }
    // found an empty slot, update it--size grows by 1
    // 发现是个空槽，放入key的散列值以及key相关信息
    bytes.position(pos)// 偏移量8字节
    bytes.put(hash1)// MD5占16字节
    bytes.putLong(offset)// 放入消息的偏移量8字节
    entries += 1
  }
  
  /**
   * Check that there is no entry at the given position
    * 检查给定位置是否没有条目
   */
  private def isEmpty(position: Int): Boolean = 
    bytes.getLong(position) == 0 && bytes.getLong(position + 8) == 0 && bytes.getLong(position + 16) == 0
  
  /**
   * Get the offset associated with this key.
   * @param key The key
   * @return The offset associated with this key or -1 if the key is not found
   */
  override def get(key: ByteBuffer): Long = {
    lookups += 1
    hashInto(key, hash1)
    // search for the hash of this key by repeated probing until we find the hash we are looking for or we find an empty slot
    var attempt = 0
    var pos = 0
    do {
      pos = positionOf(hash1, attempt)
      bytes.position(pos)
      if(isEmpty(pos))
        return -1L
      bytes.get(hash2)
      attempt += 1
    } while(!Arrays.equals(hash1, hash2))
    bytes.getLong()
  }
  
  /**
   * Change the salt used for key hashing making all existing keys unfindable.
   * Doesn't actually zero out the array.
   */
  override def clear() {
    this.entries = 0
    this.lookups = 0L
    this.probes = 0L
    Arrays.fill(bytes.array, bytes.arrayOffset, bytes.arrayOffset + bytes.limit, 0.toByte)
  }
  
  /**
   * The number of entries put into the map (note that not all may remain)
   */
  override def size: Int = entries
  
  /**
   * The rate of collisions in the lookups
   */
  def collisionRate: Double = 
    (this.probes - this.lookups) / this.lookups.toDouble
  
  /**
   * Calculate the ith probe position. We first try reading successive integers from the hash itself
   * then if all of those fail we degrade to linear probing.
   * @param hash The hash of the key to find the position for
    *             要查找位置的键的散列
   * @param attempt The ith probe
    *              第几次查找
   * @return The byte offset in the buffer at which the ith probing for the given hash would reside
    *         偏移量
   */
  private def positionOf(hash: Array[Byte], attempt: Int): Int = {
    val probe = CoreUtils.readInt(hash, math.min(attempt, hashSize - 4)) + math.max(0, attempt - hashSize + 4)
    val slot = Utils.abs(probe) % slots
    this.probes += 1
    slot * bytesPerEntry
  }
  
  /**
   * The offset at which we have stored the given key
   * @param key The key to hash
   * @param buffer The buffer to store the hash into
    * 对key进行加密记录到参数buffer中
   */
  private def hashInto(key: ByteBuffer, buffer: Array[Byte]) {
    key.mark()
    digest.update(key)
    key.reset()
    digest.digest(buffer, 0, hashSize)
  }
  
}
