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
package kafka.server

import java.nio.file.{FileSystems, Paths}
import java.util.regex.Pattern

import org.apache.kafka.common.utils.Utils

import scala.collection._
import kafka.utils.Logging
import kafka.common._
import java.io._

object OffsetCheckpoint {
  private val WhiteSpacesPattern = Pattern.compile("\\s+")
  private val CurrentVersion = 0
}

/**
 * This class saves out a map of topic/partition=>offsets to a file
  * 这个类将topic-partition=>offset的映射保存到文件中
 */
class OffsetCheckpoint(val file: File) extends Logging {
  import OffsetCheckpoint._
  // checkpoint文件
  private val path = file.toPath.toAbsolutePath
  // checkpoint临时文件
  private val tempPath = Paths.get(path.toString + ".tmp")
  private val lock = new Object()
  // 初始化的时候就创建了checkpoint文件
  file.createNewFile() // in case the file doesn't exist

  /**
    * 写检查点，大体结构如下:
    * 0
    * 53
    * __consumer_offsets 0 0
    * ... __consumer_offsets默认共50个
    * __consumer_offsets 49 0
    * simon-topic 0 1
    * test-topic 0 0
    * topic-demo 0 0
    *
    * @param offsets
    */
  def write(offsets: Map[TopicAndPartition, Long]) {
    lock synchronized {
      // write to temp file and then swap with the existing file
      // 先写临时文件然后交换临时文件和原来的RecoveryPointCheckpoint文件
      val fileOutputStream = new FileOutputStream(tempPath.toFile)
      val writer = new BufferedWriter(new OutputStreamWriter(fileOutputStream))
      try {
        // 写入版本号0，换行
        writer.write(CurrentVersion.toString)
        writer.newLine()
        // 写入topic-partition数量，换行
        writer.write(offsets.size.toString)
        writer.newLine()
        // 遍历写入所有的topic-partition和当前的offset，每写一个换一次行
        offsets.foreach { case (topicPart, offset) =>
          writer.write(s"${topicPart.topic} ${topicPart.partition} $offset")
          writer.newLine()
        }
        // 刷盘
        writer.flush()
        fileOutputStream.getFD().sync()
      } catch {
        case e: FileNotFoundException =>
          if (FileSystems.getDefault.isReadOnly) {
            fatal("Halting writes to offset checkpoint file because the underlying file system is inaccessible : ", e)
            Runtime.getRuntime.halt(1)
          }
          throw e
      } finally {
        writer.close()
      }
      // 交换
      Utils.atomicMoveWithFallback(tempPath, path)
    }
  }

  /**
    * 读取检查点
    * @return
    */
  def read(): Map[TopicAndPartition, Long] = {

    def malformedLineException(line: String) =
      new IOException(s"Malformed line in offset checkpoint file: $line'")

    lock synchronized {
      val reader = new BufferedReader(new FileReader(file))
      var line: String = null
      try {
        // 读取第一行的版本号
        line = reader.readLine()
        if (line == null)
          return Map.empty
        // 转换版本号
        val version = line.toInt
        version match {
          case CurrentVersion =>
            // 读取topic-partition的数量
            line = reader.readLine()
            // 没有，返回空Map
            if (line == null)
              return Map.empty
            // 转换topic-partition数量
            val expectedSize = line.toInt
            // 创建记录topic-partition checkpoint的offsets
            val offsets = mutable.Map[TopicAndPartition, Long]()
            // 开始循环读取topic-partition的checkpoint
            line = reader.readLine()
            while (line != null) {
              WhiteSpacesPattern.split(line) match {
                case Array(topic, partition, offset) =>
                  // 保存到offsets
                  offsets += TopicAndPartition(topic, partition.toInt) -> offset.toLong
                  line = reader.readLine()
                case _ => throw malformedLineException(line)
              }
            }
            if (offsets.size != expectedSize)
              throw new IOException(s"Expected $expectedSize entries but found only ${offsets.size}")
            // 返回
            offsets
          case _ =>
            throw new IOException("Unrecognized version of the highwatermark checkpoint file: " + version)
        }
      } catch {
        case e: NumberFormatException => throw malformedLineException(line)
      } finally {
        reader.close()
      }
    }
  }
  
}
