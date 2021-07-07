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

import kafka.api.LeaderAndIsr
import kafka.common.TopicAndPartition
import kafka.controller.{IsrChangeNotificationListener, LeaderIsrAndControllerEpoch}
import kafka.utils.ZkUtils._
import org.apache.zookeeper.data.Stat

import scala.collection._

object ReplicationUtils extends Logging {

  private val IsrChangeNotificationPrefix = "isr_change_"

  def updateLeaderAndIsr(zkUtils: ZkUtils, topic: String, partitionId: Int, newLeaderAndIsr: LeaderAndIsr, controllerEpoch: Int,
    zkVersion: Int): (Boolean,Int) = {
    debug("Updated ISR for partition [%s,%d] to %s".format(topic, partitionId, newLeaderAndIsr.isr.mkString(",")))
    // 获取topic-partition的ISR路径 /brokers/topics/{topic}/partitions/{partitionId}/state
    val path = getTopicPartitionLeaderAndIsrPath(topic, partitionId)
    // 构建新的ISR数据
    // Json.encode(Map("version" -> 1, "leader" -> leaderAndIsr.leader, "leader_epoch" -> leaderAndIsr.leaderEpoch, "controller_epoch" -> controllerEpoch, "isr" -> leaderAndIsr.isr))
    val newLeaderData = zkUtils.leaderAndIsrZkData(newLeaderAndIsr, controllerEpoch)
    // use the epoch of the controller that made the leadership decision, instead of the current controller epoch
    // 更新newLeaderData并返回成功标识以及最新的节点版本号
    val updatePersistentPath: (Boolean, Int) = zkUtils.conditionalUpdatePersistentPath(path, newLeaderData, zkVersion, Some(checkLeaderAndIsrZkData))
    updatePersistentPath
  }

  def propagateIsrChanges(zkUtils: ZkUtils, isrChangeSet: Set[TopicAndPartition]): Unit = {
    val isrChangeNotificationPath: String = zkUtils.createSequentialPersistentPath(
      // /isr_change_notification/isr_change_
      ZkUtils.IsrChangeNotificationPath + "/" + IsrChangeNotificationPrefix,
      generateIsrChangeJson(isrChangeSet))
    debug("Added " + isrChangeNotificationPath + " for " + isrChangeSet)
  }

  def checkLeaderAndIsrZkData(zkUtils: ZkUtils, path: String, expectedLeaderAndIsrInfo: String): (Boolean,Int) = {
    try {
      val writtenLeaderAndIsrInfo = zkUtils.readDataMaybeNull(path)
      val writtenLeaderOpt = writtenLeaderAndIsrInfo._1
      val writtenStat = writtenLeaderAndIsrInfo._2
      val expectedLeader = parseLeaderAndIsr(expectedLeaderAndIsrInfo, path, writtenStat)
      writtenLeaderOpt match {
        case Some(writtenData) =>
          val writtenLeader = parseLeaderAndIsr(writtenData, path, writtenStat)
          (expectedLeader,writtenLeader) match {
            case (Some(expectedLeader),Some(writtenLeader)) =>
              if(expectedLeader == writtenLeader)
                return (true,writtenStat.getVersion())
            case _ =>
          }
        case None =>
      }
    } catch {
      case e1: Exception =>
    }
    (false,-1)
  }

  // 获取topic-partition的LeaderIsrAndControllerEpoch信息
  def getLeaderIsrAndEpochForPartition(zkUtils: ZkUtils, topic: String, partition: Int):Option[LeaderIsrAndControllerEpoch] = {
    // 路径：/brokers/topics/{topic}/partitions/{partitionId}
    // 节点数据大体如下：{"controller_epoch":15,"leader":2,"version":1,"leader_epoch":25,"isr":[2,1]}
    val leaderAndIsrPath = getTopicPartitionLeaderAndIsrPath(topic, partition)
    // 读取数据
    val (leaderAndIsrOpt, stat) = zkUtils.readDataMaybeNull(leaderAndIsrPath)
    leaderAndIsrOpt.flatMap(leaderAndIsrStr => parseLeaderAndIsr(leaderAndIsrStr, leaderAndIsrPath, stat))
  }

  // 将参数封装为一个LeaderIsrAndControllerEpoch对象
  private def parseLeaderAndIsr(leaderAndIsrStr: String, path: String, stat: Stat)
      : Option[LeaderIsrAndControllerEpoch] = {
    Json.parseFull(leaderAndIsrStr).flatMap {m =>
      val leaderIsrAndEpochInfo = m.asInstanceOf[Map[String, Any]]
      val leader = leaderIsrAndEpochInfo.get("leader").get.asInstanceOf[Int]
      val epoch = leaderIsrAndEpochInfo.get("leader_epoch").get.asInstanceOf[Int]
      val isr = leaderIsrAndEpochInfo.get("isr").get.asInstanceOf[List[Int]]
      val controllerEpoch = leaderIsrAndEpochInfo.get("controller_epoch").get.asInstanceOf[Int]
      val zkPathVersion = stat.getVersion
      debug("Leader %d, Epoch %d, Isr %s, Zk path version %d for leaderAndIsrPath %s".format(leader, epoch,
        isr.toString(), zkPathVersion, path))
      Some(LeaderIsrAndControllerEpoch(LeaderAndIsr(leader, epoch, isr, zkPathVersion), controllerEpoch))}
  }

  private def generateIsrChangeJson(isrChanges: Set[TopicAndPartition]): String = {
    val partitions = isrChanges.map(tp => Map("topic" -> tp.topic, "partition" -> tp.partition)).toArray
    Json.encode(Map("version" -> IsrChangeNotificationListener.version, "partitions" -> partitions))
  }

}
