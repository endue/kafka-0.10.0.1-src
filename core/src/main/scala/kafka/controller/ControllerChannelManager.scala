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
package kafka.controller

import java.net.SocketTimeoutException
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import kafka.api._
import kafka.cluster.Broker
import kafka.common.{KafkaException, TopicAndPartition}
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.clients.{ClientRequest, ClientResponse, ManualMetadataUpdater, NetworkClient}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{ChannelBuilders, LoginType, Mode, NetworkReceive, Selectable, Selector}
import org.apache.kafka.common.protocol.{ApiKeys, SecurityProtocol}
import org.apache.kafka.common.requests
import org.apache.kafka.common.requests.{UpdateMetadataRequest, _}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{Node, TopicPartition}

import scala.collection.JavaConverters._
import scala.collection.{Set, mutable}
import scala.collection.mutable.HashMap

/**
  * 用来管理KafkaController Leader和集群中其他broker之间的网络通信
  * @param controllerContext
  * @param config
  * @param time
  * @param metrics
  * @param threadNamePrefix
  */
class ControllerChannelManager(controllerContext: ControllerContext, config: KafkaConfig, time: Time, metrics: Metrics, threadNamePrefix: Option[String] = None) extends Logging {
  // key是brokerId，value是ControllerBrokerStateInfo
  // ControllerBrokerStateInfo表示与一个broker的连续信息
  protected val brokerStateInfo = new HashMap[Int, ControllerBrokerStateInfo]
  private val brokerLock = new Object
  this.logIdent = "[Channel manager on controller " + config.brokerId + "]: "
  // 为每个broker建立ControllerBrokerStateInfo并保存到brokerStateInfo map中
  controllerContext.liveBrokers.foreach(addNewBroker(_))

  // 启动方法
  // 逻辑简单就是启动对应每个broker的RequestSendThread线程
  def startup() = {
    brokerLock synchronized {
      brokerStateInfo.foreach(brokerState => startRequestSendThread(brokerState._1))
    }
  }

  def shutdown() = {
    brokerLock synchronized {
      brokerStateInfo.values.foreach(removeExistingBroker)
    }
  }
  // 发送请求，就是将请求添加到对应broker的ControllerBrokerStateInfo的队列中
  def sendRequest(brokerId: Int, apiKey: ApiKeys, apiVersion: Option[Short], request: AbstractRequest, callback: AbstractRequestResponse => Unit = null) {
    brokerLock synchronized {
      val stateInfoOpt = brokerStateInfo.get(brokerId)
      stateInfoOpt match {
        case Some(stateInfo) =>
          stateInfo.messageQueue.put(QueueItem(apiKey, apiVersion, request, callback))
        case None =>
          warn("Not sending request %s to broker %d, since it is offline.".format(request, brokerId))
      }
    }
  }

  def addBroker(broker: Broker) {
    // be careful here. Maybe the startup() API has already started the request send thread
    brokerLock synchronized {
      if(!brokerStateInfo.contains(broker.id)) {
        addNewBroker(broker)
        startRequestSendThread(broker.id)
      }
    }
  }

  def removeBroker(brokerId: Int) {
    brokerLock synchronized {
      removeExistingBroker(brokerStateInfo(brokerId))
    }
  }

  // 添加新的broker
  private def addNewBroker(broker: Broker) {
    // 消息队列，一个broker对应一个
    val messageQueue = new LinkedBlockingQueue[QueueItem]
    debug("Controller %d trying to connect to broker %d".format(config.brokerId, broker.id))
    val brokerEndPoint = broker.getBrokerEndPoint(config.interBrokerSecurityProtocol)
    // 创建broker节点
    val brokerNode = new Node(broker.id, brokerEndPoint.host, brokerEndPoint.port)
    // 创建networkClient
    val networkClient = {
      val channelBuilder = ChannelBuilders.create(
        config.interBrokerSecurityProtocol,
        Mode.CLIENT,
        LoginType.SERVER,
        config.values,
        config.saslMechanismInterBrokerProtocol,
        config.saslInterBrokerHandshakeRequestEnable
      )
      val selector = new Selector(
        NetworkReceive.UNLIMITED,
        config.connectionsMaxIdleMs,
        metrics,
        time,
        "controller-channel",
        Map("broker-id" -> broker.id.toString).asJava,
        false,
        channelBuilder
      )
      new NetworkClient(
        selector,
        new ManualMetadataUpdater(Seq(brokerNode).asJava),
        config.brokerId.toString,
        1,
        0,
        Selectable.USE_DEFAULT_BUFFER_SIZE,
        Selectable.USE_DEFAULT_BUFFER_SIZE,
        config.requestTimeoutMs,
        time
      )
    }
    val threadName = threadNamePrefix match {
      case None => "Controller-%d-to-broker-%d-send-thread".format(config.brokerId, broker.id)
      case Some(name) => "%s:Controller-%d-to-broker-%d-send-thread".format(name, config.brokerId, broker.id)
    }
    // 创建RequestSendThread，用来发送请求
    // 这里并没有启动
    val requestThread = new RequestSendThread(config.brokerId, controllerContext, messageQueue, networkClient,
      brokerNode, config, time, threadName)
    requestThread.setDaemon(false)
    // 保存到brokerStateInfo中
    brokerStateInfo.put(broker.id, new ControllerBrokerStateInfo(networkClient, brokerNode, messageQueue, requestThread))
  }

  // 删除broker，关闭连接，清理请求队列，关闭线程，把broker相关信息从brokerStateInfo里移除
  private def removeExistingBroker(brokerState: ControllerBrokerStateInfo) {
    try {
      brokerState.networkClient.close()
      brokerState.messageQueue.clear()
      brokerState.requestSendThread.shutdown()
      brokerStateInfo.remove(brokerState.brokerNode.id)
    } catch {
      case e: Throwable => error("Error while removing broker by the controller", e)
    }
  }

  // 启动发送请求的线程
  protected def startRequestSendThread(brokerId: Int) {
    val requestThread = brokerStateInfo(brokerId).requestSendThread
    if(requestThread.getState == Thread.State.NEW)
      requestThread.start()
  }
}

// 发送出去的请求会封装为一个QueueItem
case class QueueItem(apiKey: ApiKeys, apiVersion: Option[Short], request: AbstractRequest, callback: AbstractRequestResponse => Unit)

/**
  * 发送请求的线程，一个broker对应一个
  * @param controllerId
  * @param controllerContext
  * @param queue
  * @param networkClient
  * @param brokerNode
  * @param config
  * @param time
  * @param name
  */
class RequestSendThread(val controllerId: Int,
                        val controllerContext: ControllerContext,
                        val queue: BlockingQueue[QueueItem],
                        val networkClient: NetworkClient,
                        val brokerNode: Node,
                        val config: KafkaConfig,
                        val time: Time,
                        name: String)
  extends ShutdownableThread(name = name) {

  private val lock = new Object()
  private val stateChangeLogger = KafkaController.stateChangeLogger
  private val socketTimeoutMs = config.controllerSocketTimeoutMs

  // 发送请求
  override def doWork(): Unit = {
    // 阻塞300ms
    def backoff(): Unit = CoreUtils.swallowTrace(Thread.sleep(300))
    // 获取请求队列中的消息，没有消息会阻塞住
    val QueueItem(apiKey, apiVersion, request, callback) = queue.take()
    import NetworkClientBlockingOps._
    var clientResponse: ClientResponse = null
    try {
      lock synchronized {
        var isSendSuccessful = false
        while (isRunning.get() && !isSendSuccessful) {
          // 当broker宕机后，会触发zookeeper的监听器调用removeBroker方法将当前线程停止，isRunning.get()会返回false停止重试
          // if a broker goes down for a long time, then at some point the controller's zookeeper listener will trigger a
          // removeBroker which will invoke shutdown() on this thread. At that point, we will stop retrying.
          try {
            // 如果broker连接没有准备好，那么阻塞等待
            if (!brokerReady()) {
              isSendSuccessful = false
              backoff()
            }
            else {
              // 发送请求并阻塞等待响应
              val requestHeader = apiVersion.fold(networkClient.nextRequestHeader(apiKey))(networkClient.nextRequestHeader(apiKey, _))
              val send = new RequestSend(brokerNode.idString, requestHeader, request.toStruct)
              val clientRequest = new ClientRequest(time.milliseconds(), true, send, null)
              clientResponse = networkClient.blockingSendAndReceive(clientRequest)(time)
              isSendSuccessful = true
            }
          } catch {
            // 消息发送失败，重新连接到对应broker并重新发送消息
            case e: Throwable => // if the send was not successful, reconnect to broker and resend the message
              warn(("Controller %d epoch %d fails to send request %s to broker %s. " +
                "Reconnecting to broker.").format(controllerId, controllerContext.epoch,
                  request.toString, brokerNode.toString()), e)
              networkClient.close(brokerNode.idString)
              isSendSuccessful = false
              // 阻塞等待300ms
              backoff()
          }
        }
        // 响应不为空
        if (clientResponse != null) {
          // 从这里可以看出KafkaController只能接受3种响应：
          // LeaderAndIsrResponse，StopReplicaResponse，UpdateMetadataResponse
          val response = ApiKeys.forId(clientResponse.request.request.header.apiKey) match {
            case ApiKeys.LEADER_AND_ISR => new LeaderAndIsrResponse(clientResponse.responseBody)
            case ApiKeys.STOP_REPLICA => new StopReplicaResponse(clientResponse.responseBody)
            case ApiKeys.UPDATE_METADATA_KEY => new UpdateMetadataResponse(clientResponse.responseBody)
            case apiKey => throw new KafkaException(s"Unexpected apiKey received: $apiKey")
          }
          stateChangeLogger.trace("Controller %d epoch %d received response %s for a request sent to broker %s"
            .format(controllerId, controllerContext.epoch, response.toString, brokerNode.toString))
          // 回调不为null，将响应消息发送给对应的回调
          if (callback != null) {
            callback(response)
          }
        }
      }
    } catch {
      case e: Throwable =>
        error("Controller %d fails to send a request to broker %s".format(controllerId, brokerNode.toString()), e)
        // If there is any socket error (eg, socket timeout), the connection is no longer usable and needs to be recreated.
        networkClient.close(brokerNode.idString)
    }
  }

  /**
    * 连接是否已经建立
    * @return
    */
  private def brokerReady(): Boolean = {
    import NetworkClientBlockingOps._
    try {
      // 已经建立，返回true
      if (networkClient.isReady(brokerNode, time.milliseconds()))
        true
      else {
        // 等待建立
        val ready = networkClient.blockingReady(brokerNode, socketTimeoutMs)(time)
        // 超时抛出异常
        if (!ready)
          throw new SocketTimeoutException(s"Failed to connect within $socketTimeoutMs ms")

        info("Controller %d connected to %s for sending state change requests".format(controllerId, brokerNode.toString()))
        true
      }
    } catch {
      case e: Throwable =>
        warn("Controller %d's connection to broker %s was unsuccessful".format(controllerId, brokerNode.toString()), e)
        // 抛出异常关闭连接
        networkClient.close(brokerNode.idString)
        false
    }
  }

}

/**
  * 提高KafkaController Leader和集群其他broker的通信效率，实现批量发送请求的功能
  * 这里可以看出对应三种请求leaderAndIsrRequest，stopReplicaRequest，updateMetadataRequest
  * @param controller
  */
class ControllerBrokerRequestBatch(controller: KafkaController) extends  Logging {
  val controllerContext = controller.controllerContext
  val controllerId: Int = controller.config.brokerId
  // 保存了发往指定broker的LeaderAndIsrRequest请求相关的信息
  val leaderAndIsrRequestMap = mutable.Map.empty[Int, mutable.Map[TopicPartition, PartitionStateInfo]]
  // 保存了发往指定broker的StopReplicaRequest请求相关的信息
  val stopReplicaRequestMap = mutable.Map.empty[Int, Seq[StopReplicaRequestInfo]]
  // 保存了发往指定broker的UpdateMetadataRequestMap请求相关的信息
  val updateMetadataRequestMap = mutable.Map.empty[Int, mutable.Map[TopicPartition, PartitionStateInfo]]
  private val stateChangeLogger = KafkaController.stateChangeLogger

  // 添加请求前的校验
  // 如果前一批不是空的，则引发错误
  def newBatch() {
    // raise error if the previous batch is not empty
    if (leaderAndIsrRequestMap.size > 0)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating " +
        "a new one. Some LeaderAndIsr state changes %s might be lost ".format(leaderAndIsrRequestMap.toString()))
    if (stopReplicaRequestMap.size > 0)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
        "new one. Some StopReplica state changes %s might be lost ".format(stopReplicaRequestMap.toString()))
    if (updateMetadataRequestMap.size > 0)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
        "new one. Some UpdateMetadata state changes %s might be lost ".format(updateMetadataRequestMap.toString()))
  }

  // 清空相关请求集合
  def clear() {
    leaderAndIsrRequestMap.clear()
    stopReplicaRequestMap.clear()
    updateMetadataRequestMap.clear()
  }

  // 添加LeaderAndIsrRequest
  def addLeaderAndIsrRequestForBrokers(brokerIds: Seq[Int], topic: String, partition: Int,
                                       leaderIsrAndControllerEpoch: LeaderIsrAndControllerEpoch,
                                       replicas: Seq[Int], callback: AbstractRequestResponse => Unit = null) {
    val topicPartition = new TopicPartition(topic, partition)
    // 遍历要接收请求的每一个broker
    // 获取其对应的Map[TopicPartition, PartitionStateInfo]集合，然后封装一个PartitionStateInfo请求进入
    brokerIds.filter(_ >= 0).foreach { brokerId =>
      val result = leaderAndIsrRequestMap.getOrElseUpdate(brokerId, mutable.Map.empty)
      result.put(topicPartition, PartitionStateInfo(leaderIsrAndControllerEpoch, replicas.toSet))
    }
    // 添加UpdateMetadataRequest
    addUpdateMetadataRequestForBrokers(controllerContext.liveOrShuttingDownBrokerIds.toSeq,
                                       Set(TopicAndPartition(topic, partition)))
  }
  // 添加StopReplicaRequest
  def addStopReplicaRequestForBrokers(brokerIds: Seq[Int], topic: String, partition: Int, deletePartition: Boolean,
                                      callback: (AbstractRequestResponse, Int) => Unit = null) {
    brokerIds.filter(b => b >= 0).foreach { brokerId =>
      stopReplicaRequestMap.getOrElseUpdate(brokerId, Seq.empty[StopReplicaRequestInfo])
      val v = stopReplicaRequestMap(brokerId)
      if(callback != null)
        stopReplicaRequestMap(brokerId) = v :+ StopReplicaRequestInfo(PartitionAndReplica(topic, partition, brokerId),
          deletePartition, (r: AbstractRequestResponse) => callback(r, brokerId))
      else
        stopReplicaRequestMap(brokerId) = v :+ StopReplicaRequestInfo(PartitionAndReplica(topic, partition, brokerId),
          deletePartition)
    }
  }

  /** Send UpdateMetadataRequest to the given brokers for the given partitions and partitions that are being deleted */
  // 添加UpdateMetadataRequest
  def addUpdateMetadataRequestForBrokers(brokerIds: Seq[Int],
                                         partitions: collection.Set[TopicAndPartition] = Set.empty[TopicAndPartition],
                                         callback: AbstractRequestResponse => Unit = null) {
    // 回调函数
    def updateMetadataRequestMapFor(partition: TopicAndPartition, beingDeleted: Boolean) {
      // 获取分区的LeaderIsrAndControllerEpoch
      val leaderIsrAndControllerEpochOpt = controllerContext.partitionLeadershipInfo.get(partition)
      leaderIsrAndControllerEpochOpt match {
          // 如果存在
        case Some(leaderIsrAndControllerEpoch) =>
          // 获取分区的所有副本id集合
          val replicas = controllerContext.partitionReplicaAssignment(partition).toSet
          // 封装为一个PartitionStateInfo对象
          val partitionStateInfo = if (beingDeleted) {
            // 如果是删除的话，重新生成一个LeaderAndIsr，设置leader为LeaderDuringDelete
            val leaderAndIsr = new LeaderAndIsr(LeaderAndIsr.LeaderDuringDelete, leaderIsrAndControllerEpoch.leaderAndIsr.isr)
            PartitionStateInfo(LeaderIsrAndControllerEpoch(leaderAndIsr, leaderIsrAndControllerEpoch.controllerEpoch), replicas)
          } else {
            PartitionStateInfo(leaderIsrAndControllerEpoch, replicas)
          }
          // 为每个broker都生成一个updateMetadataRequest
          brokerIds.filter(b => b >= 0).foreach { brokerId =>
            updateMetadataRequestMap.getOrElseUpdate(brokerId, mutable.Map.empty[TopicPartition, PartitionStateInfo])
            // 封装请求
            updateMetadataRequestMap(brokerId).put(new TopicPartition(partition.topic, partition.partition), partitionStateInfo)
          }
        case None =>
          info("Leader not yet assigned for partition %s. Skip sending UpdateMetadataRequest.".format(partition))
      }
    }

    // 过滤出有效的分区
    val filteredPartitions = {
      // 如果参数传递过来的为空，那么就取集合partitionLeadershipInfo中记录的所有TopicAndPartitions
      val givenPartitions = if (partitions.isEmpty)
        controllerContext.partitionLeadershipInfo.keySet
      else
        // 如果参数传递过来的不为空，返回参数TopicAndPartitions
        partitions
      // 如果partitionsToBeDeleted为空，表示没有待删除的分区，返回计算出的givenPartitions
      if (controller.deleteTopicManager.partitionsToBeDeleted.isEmpty)
        givenPartitions
      else
        // 如果partitionsToBeDeleted不为空，表示有待删除的分区，过滤待删除的分区
        givenPartitions -- controller.deleteTopicManager.partitionsToBeDeleted
    }
    // 有效的分区为空
    if (filteredPartitions.isEmpty)
      brokerIds.filter(b => b >= 0).foreach { brokerId =>
        updateMetadataRequestMap.getOrElseUpdate(brokerId, mutable.Map.empty[TopicPartition, PartitionStateInfo])
      }
    else
    // 遍历所有的分区，执行updateMetadataRequestMapFor回调方法
      filteredPartitions.foreach(partition => updateMetadataRequestMapFor(partition, beingDeleted = false))

    // 遍历待删除的分区，发送给对应的broker节点，告诉他们删除该分区
    controller.deleteTopicManager.partitionsToBeDeleted.foreach(partition => updateMetadataRequestMapFor(partition, beingDeleted = true))
  }

  // //依次取出三个map的中的消息，分别放入对应broker的queue中
  def sendRequestsToBrokers(controllerEpoch: Int) {
    try {
      // 获取leaderAndIsrRequestMap中的LEADER_AND_ISR请求
      // key是brokerId,value是Map[TopicPartition, PartitionStateInfo]
      leaderAndIsrRequestMap.foreach { case (broker, partitionStateInfos) =>
        // partitionStateInfos为Map[TopicPartition, PartitionStateInfo]类型
        partitionStateInfos.foreach { case (topicPartition, state) =>
          val typeOfRequest = if (broker == state.leaderIsrAndControllerEpoch.leaderAndIsr.leader) "become-leader" else "become-follower"
          stateChangeLogger.trace(("Controller %d epoch %d sending %s LeaderAndIsr request %s to broker %d " +
                                   "for partition [%s,%d]").format(controllerId, controllerEpoch, typeOfRequest,
                                                                   state.leaderIsrAndControllerEpoch, broker,
                                                                   topicPartition.topic, topicPartition.partition))
        }
        // 获取leaderAndIsrRequestMap集合中所有topic-partition的leader集合
        val leaderIds = partitionStateInfos.map(_._2.leaderIsrAndControllerEpoch.leaderAndIsr.leader).toSet

        val leaders = controllerContext.liveOrShuttingDownBrokers.filter(b => leaderIds.contains(b.id)).map {
          _.getNode(controller.config.interBrokerSecurityProtocol)
        }
        val partitionStates = partitionStateInfos.map { case (topicPartition, partitionStateInfo) =>
          val LeaderIsrAndControllerEpoch(leaderIsr, controllerEpoch) = partitionStateInfo.leaderIsrAndControllerEpoch
          val partitionState = new requests.PartitionState(controllerEpoch, leaderIsr.leader,
            leaderIsr.leaderEpoch, leaderIsr.isr.map(Integer.valueOf).asJava, leaderIsr.zkVersion,
            partitionStateInfo.allReplicas.map(Integer.valueOf).asJava
          )
          topicPartition -> partitionState
        }
        // 构建leaderAndIsrRequest并发送
        val leaderAndIsrRequest = new LeaderAndIsrRequest(controllerId, controllerEpoch, partitionStates.asJava, leaders.asJava)
        // 添加请求到对应broker的queue中
        controller.sendRequest(broker, ApiKeys.LEADER_AND_ISR, None, leaderAndIsrRequest, null)
      }
      // 清空leaderAndIsrRequestMap集合
      leaderAndIsrRequestMap.clear()

      // 获取updateMetadataRequestMap中的UPDATE_METADATA_KEY请求
      // key是brokerId,value是Map[TopicPartition, PartitionStateInfo]
      updateMetadataRequestMap.foreach { case (broker, partitionStateInfos) =>

        partitionStateInfos.foreach(p => stateChangeLogger.trace(("Controller %d epoch %d sending UpdateMetadata request %s " +
          "to broker %d for partition %s").format(controllerId, controllerEpoch, p._2.leaderIsrAndControllerEpoch,
          broker, p._1)))
        val partitionStates = partitionStateInfos.map { case (topicPartition, partitionStateInfo) =>
          val LeaderIsrAndControllerEpoch(leaderIsr, controllerEpoch) = partitionStateInfo.leaderIsrAndControllerEpoch
          val partitionState = new requests.PartitionState(controllerEpoch, leaderIsr.leader,
            leaderIsr.leaderEpoch, leaderIsr.isr.map(Integer.valueOf).asJava, leaderIsr.zkVersion,
            partitionStateInfo.allReplicas.map(Integer.valueOf).asJava
          )
          topicPartition -> partitionState
        }

        val version = if (controller.config.interBrokerProtocolVersion >= KAFKA_0_10_0_IV1) 2: Short
                      else if (controller.config.interBrokerProtocolVersion >= KAFKA_0_9_0) 1: Short
                      else 0: Short
        // 构建UpdateMetadataRequest请求
        val updateMetadataRequest =
          if (version == 0) {
            val liveBrokers = controllerContext.liveOrShuttingDownBrokers.map(_.getNode(SecurityProtocol.PLAINTEXT))
            new UpdateMetadataRequest(controllerId, controllerEpoch, liveBrokers.asJava, partitionStates.asJava)
          }
          else {
            // 获取所有liveBroker的信息
            val liveBrokers = controllerContext.liveOrShuttingDownBrokers.map { broker =>
              val endPoints = broker.endPoints.map { case (securityProtocol, endPoint) =>
                securityProtocol -> new UpdateMetadataRequest.EndPoint(endPoint.host, endPoint.port)
              }
              new UpdateMetadataRequest.Broker(broker.id, endPoints.asJava, broker.rack.orNull)
            }
            // 封装UpdateMetadataRequest
            new UpdateMetadataRequest(version, controllerId, controllerEpoch, partitionStates.asJava, liveBrokers.asJava)
          }
        // 添加请求到对应broker的queue中
        controller.sendRequest(broker, ApiKeys.UPDATE_METADATA_KEY, Some(version), updateMetadataRequest, null)
      }
      // 清空updateMetadataRequestMap集合
      updateMetadataRequestMap.clear()

      // 获取stopReplicaRequestMap中的STOP_REPLICA请求
      // key是brokerId,value是Map[TopicPartition, PartitionStateInfo]
      stopReplicaRequestMap.foreach { case (broker, replicaInfoList) =>
        val stopReplicaWithDelete = replicaInfoList.filter(_.deletePartition).map(_.replica).toSet
        val stopReplicaWithoutDelete = replicaInfoList.filterNot(_.deletePartition).map(_.replica).toSet
        debug("The stop replica request (delete = true) sent to broker %d is %s"
          .format(broker, stopReplicaWithDelete.mkString(",")))
        debug("The stop replica request (delete = false) sent to broker %d is %s"
          .format(broker, stopReplicaWithoutDelete.mkString(",")))
        replicaInfoList.foreach { r =>
          val stopReplicaRequest = new StopReplicaRequest(controllerId, controllerEpoch, r.deletePartition,
            Set(new TopicPartition(r.replica.topic, r.replica.partition)).asJava)
          // 添加请求到对应broker的queue中
          controller.sendRequest(broker, ApiKeys.STOP_REPLICA, None, stopReplicaRequest, r.callback)
        }
      }
      // 清空stopReplicaRequestMap
      stopReplicaRequestMap.clear()
    } catch {
      case e : Throwable => {
        if (leaderAndIsrRequestMap.size > 0) {
          error("Haven't been able to send leader and isr requests, current state of " +
              s"the map is $leaderAndIsrRequestMap. Exception message: $e")
        }
        if (updateMetadataRequestMap.size > 0) {
          error("Haven't been able to send metadata update requests, current state of " +
              s"the map is $updateMetadataRequestMap. Exception message: $e")
        }
        if (stopReplicaRequestMap.size > 0) {
          error("Haven't been able to send stop replica requests, current state of " +
              s"the map is $stopReplicaRequestMap. Exception message: $e")
        }
        throw new IllegalStateException(e)
      }
    }
  }
}

/**
  *
  * @param networkClient 负责维护controller与对应broker之间的网络连接
  * @param brokerNode 对应的broker的网络位置信息，其中保存了host、ip、port和机架信息
  * @param messageQueue 缓冲队列，缓冲发往borker的请求
  * @param requestSendThread 负责发送请求的线程
  */
case class ControllerBrokerStateInfo(networkClient: NetworkClient,
                                     brokerNode: Node,
                                     messageQueue: BlockingQueue[QueueItem],
                                     requestSendThread: RequestSendThread)

case class StopReplicaRequestInfo(replica: PartitionAndReplica, deletePartition: Boolean, callback: AbstractRequestResponse => Unit = null)

class Callbacks private (var leaderAndIsrResponseCallback: AbstractRequestResponse => Unit = null,
                         var updateMetadataResponseCallback: AbstractRequestResponse => Unit = null,
                         var stopReplicaResponseCallback: (AbstractRequestResponse, Int) => Unit = null)

object Callbacks {
  class CallbackBuilder {
    var leaderAndIsrResponseCbk: AbstractRequestResponse => Unit = null
    var updateMetadataResponseCbk: AbstractRequestResponse => Unit = null
    var stopReplicaResponseCbk: (AbstractRequestResponse, Int) => Unit = null

    def leaderAndIsrCallback(cbk: AbstractRequestResponse => Unit): CallbackBuilder = {
      leaderAndIsrResponseCbk = cbk
      this
    }

    def updateMetadataCallback(cbk: AbstractRequestResponse => Unit): CallbackBuilder = {
      updateMetadataResponseCbk = cbk
      this
    }

    def stopReplicaCallback(cbk: (AbstractRequestResponse, Int) => Unit): CallbackBuilder = {
      stopReplicaResponseCbk = cbk
      this
    }

    def build: Callbacks = {
      new Callbacks(leaderAndIsrResponseCbk, updateMetadataResponseCbk, stopReplicaResponseCbk)
    }
  }
}
