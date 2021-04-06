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
 package kafka.utils

import java.io._
import java.nio.channels._

/**
 * A file lock a la flock/funlock
 * 
 * The given path will be created and opened if it doesn't exist.
 */
// 自定义的文件锁,底层还是依赖于java.nio.channels.FileLock
class FileLock(val file: File) extends Logging {
  // 创建文件
  file.createNewFile() // create the file if it doesn't exist
  // 获取文件对应的channel,类型为FileChannel
  private val channel = new RandomAccessFile(file, "rw").getChannel()
  // java提供的文件锁
  private var flock: java.nio.channels.FileLock = null

  /**
   * Lock the file or throw an exception if the lock is already held
    * 锁定文件，如果锁已经被持有，则抛出异常
   */
  def lock() {
    this synchronized {
      trace("Acquiring lock on " + file.getAbsolutePath)
      flock = channel.lock()
    }
  }

  /**
   * Try to lock the file and return true if the locking succeeds
    * 尝试锁定文件，如果锁定成功则返回true.失败返回false
   */
  def tryLock(): Boolean = {
    this synchronized {
      trace("Acquiring lock on " + file.getAbsolutePath)
      try {
        // weirdly this method will return null if the lock is held by another
        // process, but will throw an exception if the lock is held by this process
        // so we have to handle both cases
        flock = channel.tryLock()
        flock != null
      } catch {
        case e: OverlappingFileLockException => false
      }
    }
  }

  /**
   * Unlock the lock if it is held
    * 释放锁
   */
  def unlock() {
    this synchronized {
      trace("Releasing lock on " + file.getAbsolutePath)
      if(flock != null)
        flock.release()
    }
  }

  /**
   * Destroy this lock, closing the associated FileChannel
    * 销毁此锁，关闭相关的FileChannel
   */
  def destroy() = {
    this synchronized {
      unlock()
      channel.close()
    }
  }
}