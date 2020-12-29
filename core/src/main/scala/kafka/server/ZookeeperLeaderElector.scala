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

import kafka.utils.ZkUtils._
import kafka.utils.CoreUtils._
import kafka.utils.{Json, SystemTime, Logging, ZKCheckedEphemeral}
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.I0Itec.zkclient.IZkDataListener
import kafka.controller.ControllerContext
import kafka.controller.KafkaController
import org.apache.kafka.common.security.JaasUtils

/**
 * This class handles zookeeper based leader election based on an ephemeral path. The election module does not handle
 * session expiration, instead it assumes the caller will handle it by probably try to re-elect again. If the existing
 * leader is dead, this class will handle automatic re-election and if it succeeds, it invokes the leader state change
 * callback
 */
class ZookeeperLeaderElector(controllerContext: ControllerContext,
                             electionPath: String,
                             onBecomingLeader: () => Unit,// 回调方法
                             onResigningAsLeader: () => Unit,// 回调方法
                             brokerId: Int)// brokerID
  extends LeaderElector with Logging {
  // 记录Controller的leaderID
  var leaderId = -1
  // create the election path in ZK, if one does not exist
  // 值为“/controller”
  val index = electionPath.lastIndexOf("/")
  if (index > 0)
    controllerContext.zkUtils.makeSurePersistentPathExists(electionPath.substring(0, index))
  val leaderChangeListener = new LeaderChangeListener

  def startup {
    inLock(controllerContext.controllerLock) {
      // 在“/controller”节点上注册一个监听器
      controllerContext.zkUtils.zkClient.subscribeDataChanges(electionPath, leaderChangeListener)
      // 然后开始选举
      elect
    }
  }

  /**
    * 获取“/controller”节点下的brokerID，该节点如果有内容，格式如下
    * {"version":xx,"brokerid":xx,"timestamp":"xxxxxx"}
    *
    * @return
    */
  private def getControllerID(): Int = {
    controllerContext.zkUtils.readDataMaybeNull(electionPath)._1 match {
       case Some(controller) => KafkaController.parseControllerId(controller)
       case None => -1
    }
  }

  /**
    * 选举
    * @return
    */
  def elect: Boolean = {
    val timestamp = SystemTime.milliseconds.toString
    // 封装保存到“/controller”中的信息
    val electString = Json.encode(Map("version" -> 1, "brokerid" -> brokerId, "timestamp" -> timestamp))
   // 获取leaderID
   leaderId = getControllerID 
    /* 
     * We can get here during the initial startup and the handleDeleted ZK callback. Because of the potential race condition, 
     * it's possible that the controller has already been elected when we get here. This check will prevent the following 
     * createEphemeralPath method from getting into an infinite loop if this broker is already the controller.
     */
    // 有下面几种情况会调用elect方法
    // 1.broker启动时，第一次调用
    // 2.上一次创建节点成功，但是可能在等Zookeeper响应的时候，连接中断，resign方法中删除/controller节点后，触发了leaderChangeListener的handleDataDeleted
    // 3.上一次创建节点未成功，但是可能在等Zookeeper响应的时候，连接中断，而再次进入elect方法时，已有别的broker创建controller节点成功，成为了leader
    // 4.上一次创建节点成功，但是onBecomingLeader抛出了异常，而再次进入
    // 所以先获取节点信息，判断是否已经存在

    // 如果leaderID不为-1，说明已经存在，则返回自己是否是当前leader
    if(leaderId != -1) {
       debug("Broker %d has been elected as leader, so stopping the election process.".format(leaderId))
       return amILeader
    }

    // 选举，也就是创建节点
    try {
      val zkCheckedEphemeral = new ZKCheckedEphemeral(electionPath,// 写入路径
                                                      electString,// 写入数据
                                                      controllerContext.zkUtils.zkConnection.getZookeeper,
                                                      JaasUtils.isZkSecurityEnabled())
      // 创建/controller节点，并写入controller信息，brokerid, version, timestamp
      zkCheckedEphemeral.create()
      info(brokerId + " successfully elected as leader")
      // 写入成功
      leaderId = brokerId
      // 执行回调
      onBecomingLeader()
    } catch {
      case e: ZkNodeExistsException =>
        // If someone else has written the path, then
        // 当controllerBroker已经确认
        leaderId = getControllerID 

        if (leaderId != -1)
          debug("Broker %d was elected as leader instead of broker %d".format(leaderId, brokerId))
        else
          warn("A leader has been elected but just resigned, this will result in another round of election")
      // 这里有可能是创建节点时，和zookeeper断开了连接，也有可能是onBecomingLeader的回调方法里出了异常
      // onBecomingLeader方法里，一般是初始化leader的相关的模块，如果初始化失败，则调用resign方法先删除“/controller”节点
      // 当“/controller”节点被删除时，会触发leaderChangeListener的handleDataDeleted，会重新尝试选举成Leader，
      // 更重要的是也让其他broker有机会成为leader，避免某一个broker的onBecomingLeader一直失败造成整个集群一直处于无leader的局面
      case e2: Throwable =>
        error("Error while electing or becoming leader on broker %d".format(brokerId), e2)
        // 清空
        resign()
    }
    // 返回自己是否为leader
    amILeader
  }

  def close = {
    leaderId = -1
  }

  // 判断自己是否为Controller
  def amILeader : Boolean = leaderId == brokerId

  /**
    * 删除“/controller”节点
    * @return
    */
  def resign() = {
    // 删除“/controller”节点并清空leaderId
    leaderId = -1
    controllerContext.zkUtils.deletePath(electionPath)
  }

  /**
   * We do not have session expiration listen in the ZkElection, but assuming the caller who uses this module will
   * have its own session expiration listener and handler
    * 当“/controller”节点数据发生变化时则有可能别的broker成为了leader，则调用onResigningAsLeader方法
    * 若节点被删除，则是leader已经出了故障下线了，如果当前broker之前是leader，则调用onResigningAsLeader方法，然后重新尝试选举成为leader
   */
  class LeaderChangeListener extends IZkDataListener with Logging {
    /**
     * Called when the leader information stored in zookeeper has changed. Record the new leader in memory
      * 当“/controller”节点数据发生变化时调用
     * @throws Exception On any error.
     */
    @throws(classOf[Exception])
    def handleDataChange(dataPath: String, data: Object) {
      inLock(controllerContext.controllerLock) {
        // 记录自身之前是否是controller
        val amILeaderBeforeDataChange = amILeader
        // 更新新的controller
        leaderId = KafkaController.parseControllerId(data.toString)
        info("New leader is %d".format(leaderId))
        // The old leader needs to resign leadership if it is no longer the leader
        // 如果之前是controller && 现在不是了
        if (amILeaderBeforeDataChange && !amILeader)
          onResigningAsLeader()
      }
    }

    /**
     * Called when the leader information stored in zookeeper has been delete. Try to elect as the leader
     * @throws Exception
     *             On any error.
     */
    @throws(classOf[Exception])
    def handleDataDeleted(dataPath: String) {
      inLock(controllerContext.controllerLock) {
        debug("%s leader change listener fired for path %s to handle data deleted: trying to elect as a leader"
          .format(brokerId, dataPath))
        // 如果之前是leader
        if(amILeader)
          onResigningAsLeader()
        // 重新尝试选举成为controller
        elect
      }
    }
  }
}
