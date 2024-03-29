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

import java.util

import org.apache.kafka.common.errors.{BrokerNotAvailableException, ControllerMovedException}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{AbstractRequest, AbstractRequestResponse}

import scala.collection._
import com.yammer.metrics.core.{Gauge, Meter}
import java.util.concurrent.TimeUnit

import kafka.admin.AdminUtils
import kafka.admin.PreferredReplicaLeaderElectionCommand
import kafka.api._
import kafka.cluster.Broker
import kafka.common._
import kafka.log.LogConfig
import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.utils.ZkUtils._
import kafka.utils._
import kafka.utils.CoreUtils._
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Time
import org.I0Itec.zkclient.{IZkChildListener, IZkDataListener, IZkStateListener, ZkClient, ZkConnection}
import org.I0Itec.zkclient.exception.{ZkNoNodeException, ZkNodeExistsException}
import java.util.concurrent.locks.ReentrantLock

import kafka.server._
import kafka.common.TopicAndPartition

class ControllerContext(val zkUtils: ZkUtils,
                        val zkSessionTimeout: Int) {
  // 负责和kafka集群内部broker之间建立channel来进行通信
  var controllerChannelManager: ControllerChannelManager = null

  val controllerLock: ReentrantLock = new ReentrantLock()
  // 记录关闭的brokerId集合
  var shuttingDownBrokerIds: mutable.Set[Int] = mutable.Set.empty
  val brokerShutdownLock: Object = new Object
  // 当前epoch，KafkaController的"代"，一般在leader变化后修改，首次创建为1
  // 比如当前leader挂了，新的contorller leader被选举，那么这个值就会加1，这样就能够判断哪些是旧的controller leader发送的请求
  var epoch: Int = KafkaController.InitialControllerEpoch - 1
  // 当前epoch zk上对应的版本，首次创建为1
  var epochZkVersion: Int = KafkaController.InitialControllerEpochZkVersion - 1
  // 存放集群中所有的topic
  var allTopics: Set[String] = Set.empty
  // 记录每一个topic-partition的ar集合：key是topic-partition,value是副本ids
  var partitionReplicaAssignment: mutable.Map[TopicAndPartition, Seq[Int]] = mutable.Map.empty
  // 记录每一个topic-partition的leader副本所在的brokerId、ISR列表、controller_epoch、LeaderEpoch
  var partitionLeadershipInfo: mutable.Map[TopicAndPartition, LeaderIsrAndControllerEpoch] = mutable.Map.empty
  // 记录正在重新分配副本的分区
  val partitionsBeingReassigned: mutable.Map[TopicAndPartition, ReassignedPartitionsContext] = new mutable.HashMap
  // 记录正在或需要进行"leader副本优化或选举"的topic-partition
  val partitionsUndergoingPreferredReplicaElection: mutable.Set[TopicAndPartition] = new mutable.HashSet
  // 记录潜在live的broker集合
  private var liveBrokersUnderlying: Set[Broker] = Set.empty
  // 记录当潜在live的brokerID集合
  private var liveBrokerIdsUnderlying: Set[Int] = Set.empty

  // setter
  // 根据brokers更新liveBrokersUnderlying和liveBrokerIdsUnderlying
  def liveBrokers_=(brokers: Set[Broker]) {
    liveBrokersUnderlying = brokers
    liveBrokerIdsUnderlying = liveBrokersUnderlying.map(_.id)
  }

  // getter
  // 从liveBrokersUnderlying剔除shuttingDownBrokerIds,就是可用的broker以及brokerID
  // liveBrokers：记录当前所有活跃的Broker
  def liveBrokers = liveBrokersUnderlying.filter(broker => !shuttingDownBrokerIds.contains(broker.id))
  def liveBrokerIds = liveBrokerIdsUnderlying.filter(brokerId => !shuttingDownBrokerIds.contains(brokerId))

  // 潜在live的broker以及brokerID集合
  def liveOrShuttingDownBrokerIds = liveBrokerIdsUnderlying
  def liveOrShuttingDownBrokers = liveBrokersUnderlying

  // 获取指定broker上的TopicAndPartition
  def partitionsOnBroker(brokerId: Int): Set[TopicAndPartition] = {
    partitionReplicaAssignment
      .filter { case(topicAndPartition, replicas) => replicas.contains(brokerId) }
      .map { case(topicAndPartition, replicas) => topicAndPartition }
      .toSet
  }

  // 获取指定broker上的PartitionAndReplica
  def replicasOnBrokers(brokerIds: Set[Int]): Set[PartitionAndReplica] = {
    brokerIds.map { brokerId =>
      partitionReplicaAssignment
        .filter { case(topicAndPartition, replicas) => replicas.contains(brokerId) }
        .map { case(topicAndPartition, replicas) =>
                 new PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition, brokerId) }
    }.flatten.toSet
  }

  // 获取指定topic上的PartitionAndReplica，返回Set[PartitionAndReplica]
  def replicasForTopic(topic: String): Set[PartitionAndReplica] = {
    partitionReplicaAssignment
      .filter { case(topicAndPartition, replicas) => topicAndPartition.topic.equals(topic) }
      .map { case(topicAndPartition, replicas) =>
        replicas.map { r =>
          new PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition, r)
        }
    }.flatten.toSet
  }

  // 获取指定topic上的TopicAndPartition
  def partitionsForTopic(topic: String): collection.Set[TopicAndPartition] = {
    partitionReplicaAssignment
      .filter { case(topicAndPartition, replicas) => topicAndPartition.topic.equals(topic) }.keySet
  }

  // 获取所有存活的PartitionAndReplica
  def allLiveReplicas(): Set[PartitionAndReplica] = {
    replicasOnBrokers(liveBrokerIds)
  }

  // 将参数将Set[TopicAndPartition]转换为Set[PartitionAndReplica]，获取每个分区的所有副本，然后将这些副本封装到集合中返回一个Set[PartitionAndReplica]
  def replicasForPartition(partitions: collection.Set[TopicAndPartition]): collection.Set[PartitionAndReplica] = {
    partitions.map { p =>
      val replicas = partitionReplicaAssignment(p)
      replicas.map(r => new PartitionAndReplica(p.topic, p.partition, r))
    }.flatten
  }

  // 删除指定topic并更新
  def removeTopic(topic: String) = {
    partitionLeadershipInfo = partitionLeadershipInfo.filter{ case (topicAndPartition, _) => topicAndPartition.topic != topic }
    partitionReplicaAssignment = partitionReplicaAssignment.filter{ case (topicAndPartition, _) => topicAndPartition.topic != topic }
    allTopics -= topic
  }

}


object KafkaController extends Logging {
  val stateChangeLogger = new StateChangeLogger("state.change.logger")
  val InitialControllerEpoch = 1
  val InitialControllerEpochZkVersion = 1

  case class StateChangeLogger(override val loggerName: String) extends Logging

  def parseControllerId(controllerInfoString: String): Int = {
    try {
      Json.parseFull(controllerInfoString) match {
        case Some(m) =>
          val controllerInfo = m.asInstanceOf[Map[String, Any]]
          return controllerInfo.get("brokerid").get.asInstanceOf[Int]
        case None => throw new KafkaException("Failed to parse the controller info json [%s].".format(controllerInfoString))
      }
    } catch {
      case t: Throwable =>
        // It may be due to an incompatible controller register version
        warn("Failed to parse the controller info as json. "
          + "Probably this controller is still using the old format [%s] to store the broker id in zookeeper".format(controllerInfoString))
        try {
          return controllerInfoString.toInt
        } catch {
          case t: Throwable => throw new KafkaException("Failed to parse the controller info: " + controllerInfoString + ". This is neither the new or the old format.", t)
        }
    }
  }
}

class KafkaController(val config : KafkaConfig, zkUtils: ZkUtils, val brokerState: BrokerState, time: Time, metrics: Metrics, threadNamePrefix: Option[String] = None) extends Logging with KafkaMetricsGroup {
  this.logIdent = "[Controller " + config.brokerId + "]: "
  // 当前brokerController的状态
  private var isRunning = true
  private val stateChangeLogger = KafkaController.stateChangeLogger
  // 实例化上下文
  val controllerContext = new ControllerContext(zkUtils, config.zkSessionTimeoutMs)
  // 实例化partition状态机
  val partitionStateMachine = new PartitionStateMachine(this)
  // 实例化replica状态机
  val replicaStateMachine = new ReplicaStateMachine(this)
  // 初始化ZookeeperLeaderElector对象，负责kafkaController的leader选举和故障转移
  // 为ZookeeperLeaderElector设置两个回调方法，onControllerFailover和onControllerResignation
  private val controllerElector = new ZookeeperLeaderElector(controllerContext, ZkUtils.ControllerPath, onControllerFailover, onControllerResignation, config.brokerId)
  // have a separate scheduler for the controller to be able to start and stop independently of the
  // kafka server
  // 配置"auto.leader.rebalance.enable"为true
  // 定时任务执行leader副本的重新选举
  private val autoRebalanceScheduler = new KafkaScheduler(1)
  // topic删除管理器,kafkaController才会初始化
  var deleteTopicManager: TopicDeletionManager = null
  // 分区副本leader选举的选择器
  val offlinePartitionSelector = new OfflinePartitionLeaderSelector(controllerContext, config)
  private val reassignedPartitionLeaderSelector = new ReassignedPartitionLeaderSelector(controllerContext)
  private val preferredReplicaPartitionLeaderSelector = new PreferredReplicaPartitionLeaderSelector(controllerContext)
  private val controlledShutdownPartitionLeaderSelector = new ControlledShutdownLeaderSelector(controllerContext)

  private val brokerRequestBatch = new ControllerBrokerRequestBatch(this)
  // 监听分区重分配，路径"/admin/reassign_partitions"
  // 当执行分区重分配后，会在创建上述路径并写入数据
  private val partitionReassignedListener = new PartitionsReassignedListener(this)
  // leader副本优化监听器，路径"/admin/preferred_replica_election"
  private val preferredReplicaElectionListener = new PreferredReplicaElectionListener(this)
  // ISR列表变更通知器,/isr_change_notification
  private val isrChangeNotificationListener = new IsrChangeNotificationListener(this)

  newGauge(
    "ActiveControllerCount",
    new Gauge[Int] {
      def value() = if (isActive) 1 else 0
    }
  )

  newGauge(
    "OfflinePartitionsCount",
    new Gauge[Int] {
      def value(): Int = {
        inLock(controllerContext.controllerLock) {
          if (!isActive())
            0
          else
            controllerContext.partitionLeadershipInfo.count(p => !controllerContext.liveOrShuttingDownBrokerIds.contains(p._2.leaderAndIsr.leader))
        }
      }
    }
  )

  newGauge(
    "PreferredReplicaImbalanceCount",
    new Gauge[Int] {
      def value(): Int = {
        inLock(controllerContext.controllerLock) {
          if (!isActive())
            0
          else
            controllerContext.partitionReplicaAssignment.count {
              case (topicPartition, replicas) => controllerContext.partitionLeadershipInfo(topicPartition).leaderAndIsr.leader != replicas.head
            }
        }
      }
    }
  )

  def epoch = controllerContext.epoch

  def clientId = {
    val listeners = config.listeners
    val controllerListener = listeners.get(config.interBrokerSecurityProtocol)
    "id_%d-host_%s-port_%d".format(config.brokerId, controllerListener.get.host, controllerListener.get.port)
  }

  /**
   * On clean shutdown, the controller first determines the partitions that the
   * shutting down broker leads, and moves leadership of those partitions to another broker
   * that is in that partition's ISR.
   *
   * @param id Id of the broker to shutdown.
   * @return The number of partitions that the broker still leads.
    * 关闭broker，也就是优雅关闭broker
   */
  def shutdownBroker(id: Int) : Set[TopicAndPartition] = {

    if (!isActive()) {
      throw new ControllerMovedException("Controller moved to another broker. Aborting controlled shutdown")
    }

    controllerContext.brokerShutdownLock synchronized {
      info("Shutting down broker " + id)

      inLock(controllerContext.controllerLock) {
        if (!controllerContext.liveOrShuttingDownBrokerIds.contains(id))
          throw new BrokerNotAvailableException("Broker id %d does not exist.".format(id))

        controllerContext.shuttingDownBrokerIds.add(id)
        debug("All shutting down brokers: " + controllerContext.shuttingDownBrokerIds.mkString(","))
        debug("Live brokers: " + controllerContext.liveBrokerIds.mkString(","))
      }

      val allPartitionsAndReplicationFactorOnBroker: Set[(TopicAndPartition, Int)] =
        inLock(controllerContext.controllerLock) {
          controllerContext.partitionsOnBroker(id)
            .map(topicAndPartition => (topicAndPartition, controllerContext.partitionReplicaAssignment(topicAndPartition).size))
        }

      allPartitionsAndReplicationFactorOnBroker.foreach {
        case(topicAndPartition, replicationFactor) =>
          // Move leadership serially to relinquish lock.
          inLock(controllerContext.controllerLock) {
            controllerContext.partitionLeadershipInfo.get(topicAndPartition).foreach { currLeaderIsrAndControllerEpoch =>
              if (replicationFactor > 1) {
                if (currLeaderIsrAndControllerEpoch.leaderAndIsr.leader == id) {
                  // If the broker leads the topic partition, transition the leader and update isr. Updates zk and
                  // notifies all affected brokers
                  partitionStateMachine.handleStateChanges(Set(topicAndPartition), OnlinePartition,
                    controlledShutdownPartitionLeaderSelector)
                } else {
                  // Stop the replica first. The state change below initiates ZK changes which should take some time
                  // before which the stop replica request should be completed (in most cases)
                  try {
                    brokerRequestBatch.newBatch()
                    brokerRequestBatch.addStopReplicaRequestForBrokers(Seq(id), topicAndPartition.topic,
                      topicAndPartition.partition, deletePartition = false)
                    brokerRequestBatch.sendRequestsToBrokers(epoch)
                  } catch {
                    case e : IllegalStateException => {
                      // Resign if the controller is in an illegal state
                      error("Forcing the controller to resign")
                      brokerRequestBatch.clear()
                      controllerElector.resign()

                      throw e
                    }
                  }
                  // If the broker is a follower, updates the isr in ZK and notifies the current leader
                  replicaStateMachine.handleStateChanges(Set(PartitionAndReplica(topicAndPartition.topic,
                    topicAndPartition.partition, id)), OfflineReplica)
                }
              }
            }
          }
      }
      def replicatedPartitionsBrokerLeads() = inLock(controllerContext.controllerLock) {
        trace("All leaders = " + controllerContext.partitionLeadershipInfo.mkString(","))
        controllerContext.partitionLeadershipInfo.filter {
          case (topicAndPartition, leaderIsrAndControllerEpoch) =>
            leaderIsrAndControllerEpoch.leaderAndIsr.leader == id && controllerContext.partitionReplicaAssignment(topicAndPartition).size > 1
        }.map(_._1)
      }
      replicatedPartitionsBrokerLeads().toSet
    }
  }

  /**
   * This callback is invoked by the zookeeper leader elector on electing the current broker as the new controller.
    * 这个回调是由zookeeper leader elector在选举当前broker为新的controller时调用的
    * 当成为新的leader后，要初始化leader所依赖的功能模块
   * It does the following things on the become-controller state change -
    * 当controller节点状态发生变化时，当前controller会执行如下操作
   * 1. Register controller epoch changed listener
   * 2. Increments the controller epoch
   * 3. Initializes the controller's context object that holds cache objects for current topics, live brokers and
   *    leaders for all existing partitions.
   * 4. Starts the controller's channel manager
   * 5. Starts the replica state machine
   * 6. Starts the partition state machine
   * If it encounters any unexpected exception/error while becoming controller, it resigns as the current controller.
   * This ensures another controller election will be triggered and there will always be an actively serving controller
   */
  def onControllerFailover() {
    if(isRunning) {
      info("Broker %d starting become controller state transition".format(config.brokerId))
      //read controller epoch from zk
      /**
        * 1. 读取zk上"/controller_epoch"节点记录的epoch和epochZkVersion信息
        */
      readControllerEpochFromZookeeper()

      // increment the controller epoch
      /**
        * 2. 递增zk上"/controller_epoch"节点记录的epoch并返回version
        *   2.1 如果不存在，创建并初始化
        *   2.2 如果已存在，epoch+1
        * 最后更新epochZkVersion为对应的version
        */
      incrementControllerEpoch(zkUtils.zkClient)

      // before reading source of truth from zookeeper, register the listeners to get broker/topic callbacks
      /**
        * 3. 为"/admin/reassign_partitions"节点注册partitionReassignedListener监听器
        */
      registerReassignedPartitionsListener()

      /**
        * 4. 为"/isr_change_notification"节点注册isrChangeNotificationListener监听器
        */
      registerIsrChangeNotificationListener()

      /**
        * 5. 为"/admin/preferred_replica_election"节点注册preferredReplicaElectionListener监听器
        */
      registerPreferredReplicaElectionListener()

      /**
        * 6. 为"/brokers/topics"节点注册topicChangeListener监听器
        *   如配置"delete.topic.enable"为true，则在“/admin/delete_topics”节点注册deleteTopicsListener监听器
        */
      partitionStateMachine.registerListeners()

      /**
        * 7. 在"/brokers/ids"节点注册brokerChangeListener监听器
        */
      replicaStateMachine.registerListeners()

      /**
        * 8. 初始化controllerContext
        */
      initializeControllerContext()

      /**
        * 9. 启动副本状态管理
        */
      replicaStateMachine.startup()

      /**
        * 10. 启动分区状态管理
        */
      partitionStateMachine.startup()

      // register the partition change listeners for all existing topics on failover
      /**
        * 11. 为所有的topic创建PartitionModificationsListener监听器
        */
      controllerContext.allTopics.foreach(topic => partitionStateMachine.registerPartitionChangeListener(topic))
      info("Broker %d is ready to serve as the new controller with epoch %d".format(config.brokerId, epoch))

      /**
        * 12. 设置broker状态为RunningAsController
        */
      brokerState.newState(RunningAsController)

      /**
        * 13. 为partitionsBeingReassigned集合中记录的Map[TopicAndPartition, ReassignedPartitionsContext]进行分区重分配
        */
      maybeTriggerPartitionReassignment()

      /**
        * 14. 为partitionsUndergoingPreferredReplicaElection集合中记录的Set[TopicAndPartition]进行leader选举
        */
      maybeTriggerPreferredReplicaElection()

      /* send partition leadership info to all live brokers */
      /**
        * 15. 发送UpdateMetadataRequest请求给集群中的其他broker
        */
      sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq)

      /**
        * 16. 是否配置"auto.leader.rebalance.enable"为true
        */
      if (config.autoLeaderRebalanceEnable) {
        info("starting the partition rebalance scheduler")
        // 启动autoRebalanceScheduler线程
        autoRebalanceScheduler.startup()
        // 配置"leader.imbalance.check.interval.seconds"
        autoRebalanceScheduler.schedule("partition-rebalance-thread", checkAndTriggerPartitionRebalance,
          5, config.leaderImbalanceCheckIntervalSeconds.toLong, TimeUnit.SECONDS)
      }
      // 启动topic删除管理器
      deleteTopicManager.start()
    }
    else
      info("Controller has been shut down, aborting startup/failover")
  }

  /**
   * This callback is invoked by the zookeeper leader elector when the current broker resigns as the controller. This is
   * required to clean up internal controller data structures
    * 当当前controller退出时，zookeeper leader elector会调用这个回调函数
    * 当leader退位后，要关闭leader所依赖的功能模块
   */
  def onControllerResignation() {
    debug("Controller resigning, broker id %d".format(config.brokerId))
    // de-register listeners
    deregisterIsrChangeNotificationListener()
    deregisterReassignedPartitionsListener()
    deregisterPreferredReplicaElectionListener()

    // shutdown delete topic manager
    if (deleteTopicManager != null)
      deleteTopicManager.shutdown()

    // shutdown leader rebalance scheduler
    if (config.autoLeaderRebalanceEnable)
      autoRebalanceScheduler.shutdown()

    inLock(controllerContext.controllerLock) {
      // de-register partition ISR listener for on-going partition reassignment task
      deregisterReassignedPartitionsIsrChangeListeners()
      // shutdown partition state machine
      partitionStateMachine.shutdown()
      // shutdown replica state machine
      replicaStateMachine.shutdown()
      // shutdown controller channel manager
      if(controllerContext.controllerChannelManager != null) {
        controllerContext.controllerChannelManager.shutdown()
        controllerContext.controllerChannelManager = null
      }
      // reset controller context
      controllerContext.epoch=0
      controllerContext.epochZkVersion=0
      brokerState.newState(RunningAsBroker)

      info("Broker %d resigned as the controller".format(config.brokerId))
    }
  }

  /**
   * Returns true if this broker is the current controller.
   */
  def isActive(): Boolean = {
    inLock(controllerContext.controllerLock) {
      controllerContext.controllerChannelManager != null
    }
  }

  /**
   * This callback is invoked by the replica state machine's broker change listener, with the list of newly started
   * brokers as input. It does the following -
   * 1. Sends update metadata request to all live and shutting down brokers
    * 发送update-metadata请求给所有潜在存活的broker
   * 2. Triggers the OnlinePartition state change for all new/offline partitions
    * 对于所有 new/offline partition触发选主操作,选举成功的,Partition状态设置为Online
   * 3. It checks whether there are reassigned replicas assigned to any newly started brokers.  If
   *    so, it performs the reassignment logic for each topic/partition.
    * 检查是否有重新分配的副本分配给新的broker上，如果有，就
   *
   * Note that we don't need to refresh the leader/isr cache for all topic/partitions at this point for two reasons:
   * 1. The partition state machine, when triggering online state change, will refresh leader and ISR for only those
   *    partitions currently new or offline (rather than every partition this controller is aware of)
   * 2. Even if we do refresh the cache, there is no guarantee that by the time the leader and ISR request reaches
   *    every broker that it is still valid.  Brokers check the leader epoch to determine validity of the request.
    *
    * broker上线
    * 调用 sendUpdateMetadataRequest() 方法向当前集群所有潜在存活的Broker发送UpdateMetadataRequest请求，这样的话其他的节点就会知道当前的Broker已经上线了
    * 获取当前节点分配的所有的副本列表，并将其状态转移为OnlineReplica状态
    * 触发PartitionStateMachine的 triggerOnlinePartitionStateChange() 方法，为所有处于 New/Offline的状态的分区进行leader选举，如果leader选举成功，那么该Partition的状态就会转移到OnlinePartition状态，否则状态转移失败；
    * 如果副本迁移中有新的副本落在这台新上线的节点上，那么开始执行副本迁移操作
    * 如果之前由于这个Topic设置为删除标志，但是由于其中有副本掉线而导致无法删除，这里在节点启动后，尝试重新执行删除操作
   */
  def onBrokerStartup(newBrokers: Seq[Int]) {
    info("New broker startup callback for %s".format(newBrokers.mkString(",")))
    // 新加入的broker
    val newBrokersSet = newBrokers.toSet
    // send update metadata request to all live and shutting down brokers. Old brokers will get to know of the new
    // broker via this update.
    // In cases of controlled shutdown leaders will not be elected when a new broker comes up. So at least in the
    // common controlled shutdown case, the metadata will reach the new brokers faster
    // 发送UpdateMetadataRequest到所有潜在live的broker上
    sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq)
    // the very first thing to do when a new broker comes up is send it the entire list of partitions that it is
    // supposed to host. Based on that the broker starts the high watermark threads for the input list of partitions
    // 获取新加入broker上的所有分区副本
    val allReplicasOnNewBrokers = controllerContext.replicasOnBrokers(newBrokersSet)
    // 将新添加的broker上的分区副本状态设置为OnlineReplica
    replicaStateMachine.handleStateChanges(allReplicasOnNewBrokers, OnlineReplica)
    // when a new broker comes up, the controller needs to trigger leader election for all new and offline partitions
    // to see if these brokers can become leaders for some/all of those
    // 触发new/offline partitions副本leader选举
    partitionStateMachine.triggerOnlinePartitionStateChange()
    // check if reassignment of some partitions need to be restarted
    // 检查是否有副本重分配落在这台新上线的broker
    val partitionsWithReplicasOnNewBrokers = controllerContext.partitionsBeingReassigned.filter {
      case (topicAndPartition, reassignmentContext) => reassignmentContext.newReplicas.exists(newBrokersSet.contains(_))
    }
    // 如果有副本重分配分配到了新上线的broker上，那么就执行副本重分配
    partitionsWithReplicasOnNewBrokers.foreach(p => onPartitionReassignment(p._1, p._2))
    // check if topic deletion needs to be resumed. If at least one replica that belongs to the topic being deleted exists
    // on the newly restarted brokers, there is a chance that topic deletion can resume
    // 检查 topic 删除操作是否需要重新启动
    val replicasForTopicsToBeDeleted = allReplicasOnNewBrokers.filter(p => deleteTopicManager.isTopicQueuedUpForDeletion(p.topic))
    if(replicasForTopicsToBeDeleted.size > 0) {
      info(("Some replicas %s for topics scheduled for deletion %s are on the newly restarted brokers %s. " +
        "Signaling restart of topic deletion for these topics").format(replicasForTopicsToBeDeleted.mkString(","),
        deleteTopicManager.topicsToBeDeleted.mkString(","), newBrokers.mkString(",")))
      deleteTopicManager.resumeDeletionForTopics(replicasForTopicsToBeDeleted.map(_.topic))
    }
  }

  /**
   * This callback is invoked by the replica state machine's broker change listener with the list of failed brokers
   * as input. It does the following -
   * 1. Mark partitions with dead leaders as offline
   * 2. Triggers the OnlinePartition state change for all new/offline partitions
   * 3. Invokes the OfflineReplica state change on the input list of newly started brokers
   * 4. If no partitions are effected then send UpdateMetadataRequest to live or shutting down brokers
   *
   * Note that we don't need to refresh the leader/isr cache for all topic/partitions at this point.  This is because
   * the partition state machine will refresh our cache for us when performing leader election for all new/offline
   * partitions coming online.
    *
    * broker下线
   */
  def onBrokerFailure(deadBrokers: Seq[Int]) {
    info("Broker failure callback for %s".format(deadBrokers.mkString(",")))
    // 从正在关闭的brokerId集合中剔除当前broker
    val deadBrokersThatWereShuttingDown =
      deadBrokers.filter(id => controllerContext.shuttingDownBrokerIds.remove(id))
    info("Removed %s from list of shutting down brokers.".format(deadBrokersThatWereShuttingDown))
    val deadBrokersSet = deadBrokers.toSet
    // trigger OfflinePartition state for all partitions whose current leader is one amongst the dead brokers
    // 过滤出leader在下线broker的并且没有设置为删除的分区
    val partitionsWithoutLeader = controllerContext.partitionLeadershipInfo.filter(partitionAndLeader =>
      deadBrokersSet.contains(partitionAndLeader._2.leaderAndIsr.leader) &&
        !deleteTopicManager.isTopicQueuedUpForDeletion(partitionAndLeader._1.topic)).keySet
    // 将上述过滤出来的分区状态设置为OfflinePartition
    partitionStateMachine.handleStateChanges(partitionsWithoutLeader, OfflinePartition)
    // trigger OnlinePartition state changes for offline or new partitions
    // 触发offline/new partition状态为OnlinePartition
    // 也就是为上述失去leader的分区进行leader的选举
    partitionStateMachine.triggerOnlinePartitionStateChange()
    // filter out the replicas that belong to topics that are being deleted
    // 过滤出副本在这个broker上并且没有被设置为删除的topic集合
    var allReplicasOnDeadBrokers = controllerContext.replicasOnBrokers(deadBrokersSet)
    // 过滤掉等待删除的topic
    val activeReplicasOnDeadBrokers = allReplicasOnDeadBrokers.filterNot(p => deleteTopicManager.isTopicQueuedUpForDeletion(p.topic))
    // handle dead replicas
    // 将上述过滤出来的副本，也就是在要下线broker上的副本的状态设置为OfflineReplica
    replicaStateMachine.handleStateChanges(activeReplicasOnDeadBrokers, OfflineReplica)
    // check if topic deletion state for the dead replicas needs to be updated
    // 过滤出副本在这个broker上并且被设置为删除的topic集合
    val replicasForTopicsToBeDeleted = allReplicasOnDeadBrokers.filter(p => deleteTopicManager.isTopicQueuedUpForDeletion(p.topic))
    // 如果被删除topic有副本在要下线的broker上
    if(replicasForTopicsToBeDeleted.size > 0) {
      // it is required to mark the respective replicas in TopicDeletionFailed state since the replica cannot be
      // deleted when the broker is down. This will prevent the replica from being in TopicDeletionStarted state indefinitely
      // since topic deletion cannot be retried until at least one replica is in TopicDeletionStarted state
      // 将上述过滤出来的副本状态设置为ReplicaDeletionIneligible
      // 然后再将该Topic标记为暂停删除，即因为有副本掉线导致该Topic无法删除
      deleteTopicManager.failReplicaDeletion(replicasForTopicsToBeDeleted)
    }

    // If broker failure did not require leader re-election, inform brokers of failed broker
    // Note that during leader re-election, brokers update their metadata
    // 有分区leader在下线broker，需要通知其他broker更新缓存
    if (partitionsWithoutLeader.isEmpty) {
      sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq)
    }
  }

  /**
    * 当新的topic创建时被调用执行
    * 参考 {@link kafka.controller.PartitionStateMachine.TopicChangeListener}
   * This callback is invoked by the partition state machine's topic change listener with the list of new topics
   * and partitions as input. It does the following -
   * 1. Registers partition change listener. This is not required until KAFKA-347
    * 注册分区监听器
   * 2. Invokes the new partition callback
    * 执行新分区的回调
   * 3. Send metadata request with the new topic to all brokers so they allow requests for that topic to be served
    * 发送带有新topic的元数据请求给集群中其他的服务,以便他们能够提供对该topic请求的服务
    * @param topics 新topic集合
    * @param newPartitions 新topic-partition集合
   */
  def onNewTopicCreation(topics: Set[String], newPartitions: Set[TopicAndPartition]) {
    info("New topic creation callback for %s".format(newPartitions.mkString(",")))
    // subscribe to partition changes
    // 为新topic注册registerPartitionChangeListener监听器
    topics.foreach(topic => partitionStateMachine.registerPartitionChangeListener(topic))
    // 处理新的topic对应的分区
    onNewPartitionCreation(newPartitions)
  }

  /**
    * 新的分区创建
   * This callback is invoked by the topic change callback with the list of failed brokers as input.
   * It does the following -
   * 1. Move the newly created partitions to the NewPartition state
    * 将新创建的分区移动到NewPartition状态
   * 2. Move the newly created partitions from NewPartition->OnlinePartition state
    * 将新创建的分区从NewPartition->OnlinePartition状态
    * @param newPartitions 新topic-partition集合
   */
  def onNewPartitionCreation(newPartitions: Set[TopicAndPartition]) {
    info("New partition creation callback for %s".format(newPartitions.mkString(",")))

    /**
      * 1. 通过分区状态机将新Partitions的状态转为NewPartition
      */
    partitionStateMachine.handleStateChanges(newPartitions, NewPartition)

    /**
      * 2. 通过副本状态机将新Partitions的副本状态转为NewReplica
      */
    replicaStateMachine.handleStateChanges(controllerContext.replicasForPartition(newPartitions), NewReplica)

    /**
      * 3. 通过分区状态机将新Partitions的状态转为OnlinePartition
      */
    partitionStateMachine.handleStateChanges(newPartitions, OnlinePartition, offlinePartitionSelector)

    /**
      * 4. 通过副本状态机将新Partitions的副本状态转为OnlineReplica
      */
    replicaStateMachine.handleStateChanges(controllerContext.replicasForPartition(newPartitions), OnlineReplica)
  }

  /**
   * This callback is invoked by the reassigned partitions listener. When an admin command initiates a partition
   * reassignment, it creates the /admin/reassign_partitions path that triggers the zookeeper listener.
    * 这个回调方法被reassigned partitions listener触发,当需要进行分区副本迁移时,会在"/admin/reassign_partitions"下创建一个节点来触发操作
   * Reassigning replicas for a partition goes through a few steps listed in the code.
   * RAR = Reassigned replicas 新分配的副本列表
   * OAR = Original list of replicas for partition 原来的副本列表
   * AR = current assigned replicas 当前副本列表
   *
   * 1. Update AR in ZK with OAR + RAR.
    * 用OAR + RAR更新ZK中的AR
   * 2. Send LeaderAndIsr request to every replica in OAR + RAR (with AR as OAR + RAR). We do this by forcing an update
   *    of the leader epoch in zookeeper.
    * 发送LeaderAndIsr请求到每个AR副本，通过强制更新zk中的leader epoch来实现
   * 3. Start new replicas RAR - OAR by moving replicas in RAR - OAR to NewReplica state.
    * 启动新的副本，状态设置为NewReplica
   * 4. Wait until all replicas in RAR are in sync with the leader.
    * 等待直到RAR中的所有副本与leader同步
   * 5  Move all replicas in RAR to OnlineReplica state.
    * 移动RAR中所有部分到OnlineReplica状态
   * 6. Set AR to RAR in memory.
    * 更新内存中的RAR为AR
   * 7. If the leader is not in RAR, elect a new leader from RAR. If new leader needs to be elected from RAR, a LeaderAndIsr
   *    will be sent. If not, then leader epoch will be incremented in zookeeper and a LeaderAndIsr request will be sent.
   *    In any case, the LeaderAndIsr request will have AR = RAR. This will prevent the leader from adding any replica in
   *    RAR - OAR back in the isr.
    * 如果leader不在RAR中，那么就从RAR中选择一个leader，之后发送一个LeaderAndIsr请求
    * 如果leader在RAR中，那么只需要递增zk中的eader epoch，之后发送一个LeaderAndIsr请求
    * 在发送LeaderAndIsr请求前设置了AR=RAR, 这将确保了leader在isr中不会添加任何RAR-OAR中的副本
   * 8. Move all replicas in OAR - RAR to OfflineReplica state. As part of OfflineReplica state change, we shrink the
   *    isr to remove OAR - RAR in zookeeper and sent a LeaderAndIsr ONLY to the Leader to notify it of the shrunk isr.
   *    After that, we send a StopReplica (delete = false) to the replicas in OAR - RAR.
    * 将OAR - RAR的所有副本都设置为OfflineReplica状态，OfflineReplica状态的变化将会从zk的isr列表中删除OAR - RAR的副本
    * 并发送一个LeaderAndIsr请求给leader副本，通知它，isr列表发生了变化
    * 最后再发送一个StopReplica (delete = false)请求到OAR - RAR副本
   * 9. Move all replicas in OAR - RAR to NonExistentReplica state. This will send a StopReplica (delete = false) to
   *    the replicas in OAR - RAR to physically delete the replicas on disk.
    * 将OAR - RAR的所有副本设置为NonExistentReplica状态，发送StopReplica (delete = false)请求到OAR - RAR副本然后物理删除
    * 这些副本上保存的分区数据
   * 10. Update AR in ZK with RAR.
    * 更新zk上的RAR为AR
   * 11. Update the /admin/reassign_partitions path in ZK to remove this partition.
    * 更新zk中"/admin/reassign_partitions"信息，移除已经成功迁移的分区
   * 12. After electing leader, the replicas and isr information changes. So resend the update metadata request to every broker.
    * leader选举完成后，这个副本的isr信息会发生变化，然后发送update metadata request到每个broker上
   *
   * For example, if OAR = {1, 2, 3} and RAR = {4,5,6}, the values in the assigned replica (AR) and leader/isr path in ZK
   * may go through the following transition.
    * 举例，OAR = {1, 2, 3} ，RAR = {4,5,6}
   * AR                 leader/isr
   * {1,2,3}            1/{1,2,3}           (initial state)
   * {1,2,3,4,5,6}      1/{1,2,3}           (step 2)
   * {1,2,3,4,5,6}      1/{1,2,3,4,5,6}     (step 4)
   * {1,2,3,4,5,6}      4/{1,2,3,4,5,6}     (step 7)
   * {1,2,3,4,5,6}      4/{4,5,6}           (step 8)
   * {4,5,6}            4/{4,5,6}           (step 10)
   *
   * Note that we have to update AR in ZK with RAR last since it's the only place where we store OAR persistently.
   * This way, if the controller crashes before that step, we can still recover.
   */
  // 真正的执行分区重分配
  def onPartitionReassignment(topicAndPartition: TopicAndPartition, reassignedPartitionContext: ReassignedPartitionsContext) {
    // 新副本
    val reassignedReplicas = reassignedPartitionContext.newReplicas
    // 遍历新分配的副本是否在ISR列表中
    // 如果否执行步骤1--3
    areReplicasInIsr(topicAndPartition.topic, topicAndPartition.partition, reassignedReplicas) match {
        // 否
      case false =>
        info("New replicas %s for partition %s being ".format(reassignedReplicas.mkString(","), topicAndPartition) +
          "reassigned not yet caught up with the leader")
        // newReplicasNotInOldReplicaList = 计算新增加的副本 = RAR - OAR
        val newReplicasNotInOldReplicaList = reassignedReplicas.toSet -- controllerContext.partitionReplicaAssignment(topicAndPartition).toSet
        // newAndOldReplicas = 新副本 + 旧副本 = OAR + RAR.
        val newAndOldReplicas = (reassignedPartitionContext.newReplicas ++ controllerContext.partitionReplicaAssignment(topicAndPartition)).toSet
        //1. Update AR in ZK with OAR + RAR.
        // 1.更新zk和本地缓存中的ar列表为OAR + RAR
        updateAssignedReplicasForPartition(topicAndPartition, newAndOldReplicas.toSeq)
        //2. Send LeaderAndIsr request to every replica in OAR + RAR (with AR as OAR + RAR).
        // 向上面AR(OAR+RAR)中的副本发送LeaderAndIsr请求
        updateLeaderEpochAndSendRequest(topicAndPartition, controllerContext.partitionReplicaAssignment(topicAndPartition),
          newAndOldReplicas.toSeq)
        //3. replicas in RAR - OAR -> NewReplica
        // 更新新副本的状态为NewReplica
        startNewReplicasForReassignedPartition(topicAndPartition, reassignedPartitionContext, newReplicasNotInOldReplicaList)
        info("Waiting for new replicas %s for partition %s being ".format(reassignedReplicas.mkString(","), topicAndPartition) +
          "reassigned to catch up with the leader")
        // 是
      case true =>
        //4. Wait until all replicas in RAR are in sync with the leader.
        // oldReplicas = 旧副本 - 新副本 = OAR - RAR
        val oldReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition).toSet -- reassignedReplicas.toSet
        //5. replicas in RAR -> OnlineReplica
        // 更新RAR中副本状态为OnlineReplica
        reassignedReplicas.foreach { replica =>
          replicaStateMachine.handleStateChanges(Set(new PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition,
            replica)), OnlineReplica)
        }
        //6. Set AR to RAR in memory.
        //7. Send LeaderAndIsr request with a potential new leader (if current leader not in RAR) and
        //   a new AR (using RAR) and same isr to every broker in RAR
        // 6.将内存缓存中的AR设置为RAR
        // 7.选择leader，发送LeaderAndIsr请求
        moveReassignedPartitionLeaderIfRequired(topicAndPartition, reassignedPartitionContext)
        //8. replicas in OAR - RAR -> Offline (force those replicas out of isr)
        //9. replicas in OAR - RAR -> NonExistentReplica (force those replicas to be deleted)
        // 8-9.将旧的副本下线
        stopOldReplicasOfReassignedPartition(topicAndPartition, reassignedPartitionContext, oldReplicas)
        //10. Update AR in ZK with RAR.
        // 将ZK中的AR设置为RAR
        updateAssignedReplicasForPartition(topicAndPartition, reassignedReplicas)
        //11. Update the /admin/reassign_partitions path in ZK to remove this partition.
        // 分区重分配完成, 从zk'/admin/reassign_partitions'节点删除分区
        removePartitionFromReassignedPartitions(topicAndPartition)
        info("Removed partition %s from the list of reassigned partitions in zookeeper".format(topicAndPartition))
        controllerContext.partitionsBeingReassigned.remove(topicAndPartition)
        //12. After electing leader, the replicas and isr information changes, so resend the update metadata request to every broker
        // 发送metadata更新请求给所有存活的broker
        sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set(topicAndPartition))
        // signal delete topic thread if reassignment for some partitions belonging to topics being deleted just completed
        // 如果topic标记了删除, 此时唤醒删除线程
        deleteTopicManager.resumeDeletionForTopics(Set(topicAndPartition.topic))
    }
  }

  private def watchIsrChangesForReassignedPartition(topic: String,
                                                    partition: Int,
                                                    reassignedPartitionContext: ReassignedPartitionsContext) {
    val reassignedReplicas = reassignedPartitionContext.newReplicas
    val isrChangeListener = new ReassignedPartitionsIsrChangeListener(this, topic, partition,
      reassignedReplicas.toSet)
    reassignedPartitionContext.isrChangeListener = isrChangeListener
    // register listener on the leader and isr path to wait until they catch up with the current leader
    zkUtils.zkClient.subscribeDataChanges(getTopicPartitionLeaderAndIsrPath(topic, partition), isrChangeListener)
  }

  /**
    * 分区副本重分配
    * @param topicAndPartition
    * @param reassignedPartitionContext
    */
  def initiateReassignReplicasForTopicPartition(topicAndPartition: TopicAndPartition,
                                        reassignedPartitionContext: ReassignedPartitionsContext) {
    // 获取新的副本列表
    val newReplicas = reassignedPartitionContext.newReplicas
    // 获取要重分配的topic
    val topic = topicAndPartition.topic
    // 获取要重分配的分区
    val partition = topicAndPartition.partition
    // 从新的副本中过滤出活跃的副本
    val aliveNewReplicas = newReplicas.filter(r => controllerContext.liveBrokerIds.contains(r))
    try {
      // 获取分区旧的ar副本列表
      val assignedReplicasOpt = controllerContext.partitionReplicaAssignment.get(topicAndPartition)
      assignedReplicasOpt match {
        case Some(assignedReplicas) =>
          // 如果要分配的分区新分配的副本列表与之前的副本列表相同，那么不进行副本重分配
          if(assignedReplicas == newReplicas) {
            throw new KafkaException("Partition %s to be reassigned is already assigned to replicas".format(topicAndPartition) +
              " %s. Ignoring request for partition reassignment".format(newReplicas.mkString(",")))
          } else {
            // 新分配的副本都是活跃的
            if(aliveNewReplicas == newReplicas) {
              info("Handling reassignment of partition %s to new replicas %s".format(topicAndPartition, newReplicas.mkString(",")))
              // first register ISR change listener
              // 注册isr列表监听器
              watchIsrChangesForReassignedPartition(topic, partition, reassignedPartitionContext)
              // 更新副本标记为正在重新分配副本
              controllerContext.partitionsBeingReassigned.put(topicAndPartition, reassignedPartitionContext)
              // mark topic ineligible for deletion for the partitions being reassigned
              // 标记当前主题不可用删除
              deleteTopicManager.markTopicIneligibleForDeletion(Set(topic))
              // 执行分区重分配
              onPartitionReassignment(topicAndPartition, reassignedPartitionContext)
            } else {
              // some replica in RAR is not alive. Fail partition reassignment
              throw new KafkaException("Only %s replicas out of the new set of replicas".format(aliveNewReplicas.mkString(",")) +
                " %s for partition %s to be reassigned are alive. ".format(newReplicas.mkString(","), topicAndPartition) +
                "Failing partition reassignment")
            }
          }
        case None => throw new KafkaException("Attempt to reassign partition %s that doesn't exist"
          .format(topicAndPartition))
      }
    } catch {
      case e: Throwable => error("Error completing reassignment of partition %s".format(topicAndPartition), e)
      // remove the partition from the admin path to unblock the admin client
      removePartitionFromReassignedPartitions(topicAndPartition)
    }
  }

  def onPreferredReplicaElection(partitions: Set[TopicAndPartition], isTriggeredByAutoRebalance: Boolean = false) {
    info("Starting preferred replica leader election for partitions %s".format(partitions.mkString(",")))
    try {
      // 记录当前分区
      controllerContext.partitionsUndergoingPreferredReplicaElection ++= partitions
      // 暂停当前topic的删除操作
      deleteTopicManager.markTopicIneligibleForDeletion(partitions.map(_.topic))
      // 更新当前分区的状态为OnlinePartition
      partitionStateMachine.handleStateChanges(partitions, OnlinePartition, preferredReplicaPartitionLeaderSelector)
    } catch {
      case e: Throwable => error("Error completing preferred replica leader election for partitions %s".format(partitions.mkString(",")), e)
    } finally {
      removePartitionsFromPreferredReplicaElection(partitions, isTriggeredByAutoRebalance)
      deleteTopicManager.resumeDeletionForTopics(partitions.map(_.topic))
    }
  }

  /**
   * Invoked when the controller module of a Kafka server is started up. This does not assume that the current broker
   * is the controller. It merely registers the session expiration listener and starts the controller leader
   * elector
   */
  def startup() = {
    inLock(controllerContext.controllerLock) {
      info("Controller starting up")
      // 注册Session过期监听器，主要是监听当自身session过期后释放一些资源然后重新进行leader选举
      registerSessionExpirationListener()
      isRunning = true
      // 尝试选举leader
      controllerElector.startup
      info("Controller startup complete")
    }
  }

  /**
   * Invoked when the controller module of a Kafka server is shutting down. If the broker was the current controller,
   * it shuts down the partition and replica state machines. If not, those are a no-op. In addition to that, it also
   * shuts down the controller channel manager, if one exists (i.e. if it was the current controller)
   */
  def shutdown() = {
    inLock(controllerContext.controllerLock) {
      isRunning = false
    }
    onControllerResignation()
  }

  def sendRequest(brokerId: Int, apiKey: ApiKeys, apiVersion: Option[Short], request: AbstractRequest, callback: AbstractRequestResponse => Unit = null) = {
    controllerContext.controllerChannelManager.sendRequest(brokerId, apiKey, apiVersion, request, callback)
  }

  /**
    * 递增epoch
    * @param zkClient
    */
  def incrementControllerEpoch(zkClient: ZkClient) = {
    try {
      // 增加epoch
      var newControllerEpoch = controllerContext.epoch + 1
      // 更新zk上的记录
      val (updateSucceeded, newVersion) = zkUtils.conditionalUpdatePersistentPathIfExists(
        ZkUtils.ControllerEpochPath, newControllerEpoch.toString, controllerContext.epochZkVersion)
      // 执行失败报错
      if(!updateSucceeded)
        throw new ControllerMovedException("Controller moved to another broker. Aborting controller startup procedure")
      // 执行成功则更新本地对应的值为最新值
      else {
        controllerContext.epochZkVersion = newVersion
        controllerContext.epoch = newControllerEpoch
      }
    } catch {
      case nne: ZkNoNodeException =>
        // if path doesn't exist, this is the first controller whose epoch should be 1
        // 如果path不存在，这是第一个epoch为1的控制器
        // the following call can still fail if another controller gets elected between checking if the path exists and
        // trying to create the controller epoch path
        try {
          // 创建持久epoch节点，写入epoch和epochzkVersion
          zkClient.createPersistent(ZkUtils.ControllerEpochPath, KafkaController.InitialControllerEpoch.toString)
          controllerContext.epoch = KafkaController.InitialControllerEpoch
          controllerContext.epochZkVersion = KafkaController.InitialControllerEpochZkVersion
        } catch {
          case e: ZkNodeExistsException => throw new ControllerMovedException("Controller moved to another broker. " +
            "Aborting controller startup procedure")
          case oe: Throwable => error("Error while incrementing controller epoch", oe)
        }
      case oe: Throwable => error("Error while incrementing controller epoch", oe)

    }
    info("Controller %d incremented epoch to %d".format(config.brokerId, controllerContext.epoch))
  }

  // 注册SessionExpirationListener监听器
  private def registerSessionExpirationListener() = {
    zkUtils.zkClient.subscribeStateChanges(new SessionExpirationListener())
  }

  // 初始化kafakController上下文
  private def initializeControllerContext() {
    // update controller cache with delete topic information
    // 读取"/brokers/ids"节点下的所有brokerId并过滤出包含实例信息的broker
    // 赋值给liveBrokers
    controllerContext.liveBrokers = zkUtils.getAllBrokersInCluster().toSet
    // 读取zk"/brokers/topics"路径下所有的topic
    controllerContext.allTopics = zkUtils.getAllTopics().toSet
    // 读取每个topic的分配结果
    controllerContext.partitionReplicaAssignment = zkUtils.getReplicaAssignmentForTopics(controllerContext.allTopics.toSeq)
    // 初始化partitionLeadershipInfo集合
    controllerContext.partitionLeadershipInfo = new mutable.HashMap[TopicAndPartition, LeaderIsrAndControllerEpoch]
    // 初始化shuttingDownBrokerIds集合
    controllerContext.shuttingDownBrokerIds = mutable.Set.empty[Int]
    // update the leader and isr cache for all existing partitions from Zookeeper
    // 读取zk上的数据并更新partitionLeadershipInfo中记录的数据
    updateLeaderAndIsrCache()
    // start the channel manager
    // 初始化ControllerChannelManager并启动
    // 逻辑大概就是与其他broker建立通信并启动发送线程等等......
    startChannelManager()
    // 更新partitionsUndergoingPreferredReplicaElection的数据，保留可以选举leader的topic-partition
    initializePreferredReplicaElection()
    // 记录哪些topic-partition需要分区重分配到partitionsBeingReassigned集合中
    initializePartitionReassignment()
    // 计算需要被删除的topic
    // 并初始化TopicDeletionManager
    initializeTopicDeletion()
    info("Currently active brokers in the cluster: %s".format(controllerContext.liveBrokerIds))
    info("Currently shutting brokers in the cluster: %s".format(controllerContext.shuttingDownBrokerIds))
    info("Current list of topics in the cluster: %s".format(controllerContext.allTopics))
  }

  // 更新partitionsUndergoingPreferredReplicaElection的数据，保留可以选举leader的topic-partition
  private def initializePreferredReplicaElection() {
    // initialize preferred replica election state
    // 读取zk "/admin/preferred_replica_election"节点下数据，返回的是topic-partition集合
    val partitionsUndergoingPreferredReplicaElection = zkUtils.getPartitionsUndergoingPreferredReplicaElection()
    // check if they are already completed or topic was deleted
    // 检查这些topic-partition已经完成leader选举或topic已经被删除
    // 也就是过滤出那些topic被删除或已经完成leader选举的topic-partition
    val partitionsThatCompletedPreferredReplicaElection = partitionsUndergoingPreferredReplicaElection.filter { partition =>
      val replicasOpt = controllerContext.partitionReplicaAssignment.get(partition)
      val topicDeleted = replicasOpt.isEmpty
      val successful =
        // 如果当前topic-partition对应的topic没有被删除并且leader节点存在，那么说明已经完成leader选项
        if(!topicDeleted) controllerContext.partitionLeadershipInfo(partition).leaderAndIsr.leader == replicasOpt.get.head else false
      successful || topicDeleted
    }
    // 更新partitionsUndergoingPreferredReplicaElection集合，记录等待leader选择的topic-partition
    controllerContext.partitionsUndergoingPreferredReplicaElection ++= partitionsUndergoingPreferredReplicaElection
    controllerContext.partitionsUndergoingPreferredReplicaElection --= partitionsThatCompletedPreferredReplicaElection
    info("Partitions undergoing preferred replica election: %s".format(partitionsUndergoingPreferredReplicaElection.mkString(",")))
    info("Partitions that completed preferred replica election: %s".format(partitionsThatCompletedPreferredReplicaElection.mkString(",")))
    info("Resuming preferred replica election for partitions: %s".format(controllerContext.partitionsUndergoingPreferredReplicaElection.mkString(",")))
  }

  // 记录哪些topic-partition需要分区重分配到partitionsBeingReassigned集合中
  private def initializePartitionReassignment() {
    // read the partitions being reassigned from zookeeper path /admin/reassign_partitions
    // 读取zk "/admin/reassign_partitions" 节点下数据
    // 返回数据Map[TopicAndPartition, ReassignedPartitionsContext]
    val partitionsBeingReassigned = zkUtils.getPartitionsBeingReassigned()
    // check if they are already completed or topic was deleted
    // 检查这些topic-partition已经完成leader选举或topic已经被删除
    // 过滤出不需要重新分配的topic-partition
    val reassignedPartitions = partitionsBeingReassigned.filter { partition =>
      val replicasOpt = controllerContext.partitionReplicaAssignment.get(partition._1)
      val topicDeleted = replicasOpt.isEmpty
      val successful = if(!topicDeleted) replicasOpt.get == partition._2.newReplicas else false
      topicDeleted || successful
    }.map(_._1)
    reassignedPartitions.foreach(p => removePartitionFromReassignedPartitions(p))
    var partitionsToReassign: mutable.Map[TopicAndPartition, ReassignedPartitionsContext] = new mutable.HashMap
    // 更新partitionsBeingReassigned集合，记录等待重新分配分区的topic-partition
    partitionsToReassign ++= partitionsBeingReassigned
    partitionsToReassign --= reassignedPartitions
    controllerContext.partitionsBeingReassigned ++= partitionsToReassign
    info("Partitions being reassigned: %s".format(partitionsBeingReassigned.toString()))
    info("Partitions already reassigned: %s".format(reassignedPartitions.toString()))
    info("Resuming reassignment of partitions: %s".format(partitionsToReassign.toString()))
  }

  // 计算需要被删除的topic
  // 并初始化TopicDeletionManager
  private def initializeTopicDeletion() {
    // 读取zk "/admin/delete_topics" 节点上的数据
    val topicsQueuedForDeletion = zkUtils.getChildrenParentMayNotExist(ZkUtils.DeleteTopicsPath).toSet
    // 过滤出副本在Dead状态broker上的topic-partition对应的topic
    val topicsWithReplicasOnDeadBrokers = controllerContext.partitionReplicaAssignment.filter { case(partition, replicas) =>
      replicas.exists(r => !controllerContext.liveBrokerIds.contains(r)) }.keySet.map(_.topic)
    // 获取正在或需要进行"leader副本优化或选举"的topic-partition对应的topic
    val topicsForWhichPreferredReplicaElectionIsInProgress = controllerContext.partitionsUndergoingPreferredReplicaElection.map(_.topic)
    // 获取正在重新分配副本的topic-partition的topic
    val topicsForWhichPartitionReassignmentIsInProgress = controllerContext.partitionsBeingReassigned.keySet.map(_.topic)
    // 计算不适合被删除的topic
    // 当前topic-partition对应的副本宕机、正在leader选举、正在副本分配
    val topicsIneligibleForDeletion = topicsWithReplicasOnDeadBrokers | topicsForWhichPartitionReassignmentIsInProgress |
                                  topicsForWhichPreferredReplicaElectionIsInProgress
    info("List of topics to be deleted: %s".format(topicsQueuedForDeletion.mkString(",")))
    info("List of topics ineligible for deletion: %s".format(topicsIneligibleForDeletion.mkString(",")))
    // initialize the topic deletion manager
    // 初始化TopicDeletionManager
    deleteTopicManager = new TopicDeletionManager(this, topicsQueuedForDeletion, topicsIneligibleForDeletion)
  }

  private def maybeTriggerPartitionReassignment() {
    controllerContext.partitionsBeingReassigned.foreach { topicPartitionToReassign =>
      initiateReassignReplicasForTopicPartition(topicPartitionToReassign._1, topicPartitionToReassign._2)
    }
  }

  private def maybeTriggerPreferredReplicaElection() {
    onPreferredReplicaElection(controllerContext.partitionsUndergoingPreferredReplicaElection.toSet)
  }
  // 初始化ControllerChannelManager并启动
  // 逻辑大概就是与其他broker建立通信并启动发送线程等等......
  private def startChannelManager() {
    controllerContext.controllerChannelManager = new ControllerChannelManager(controllerContext, config, time, metrics, threadNamePrefix)
    controllerContext.controllerChannelManager.startup()
  }
  // 读取zk上的数据并更新partitionLeadershipInfo中记录的数据
  def updateLeaderAndIsrCache(topicAndPartitions: Set[TopicAndPartition] = controllerContext.partitionReplicaAssignment.keySet) {
    val leaderAndIsrInfo = zkUtils.getPartitionLeaderAndIsrForTopics(zkUtils.zkClient, topicAndPartitions)
    for((topicPartition, leaderIsrAndControllerEpoch) <- leaderAndIsrInfo)
      controllerContext.partitionLeadershipInfo.put(topicPartition, leaderIsrAndControllerEpoch)
  }

  private def areReplicasInIsr(topic: String, partition: Int, replicas: Seq[Int]): Boolean = {
    zkUtils.getLeaderAndIsrForPartition(topic, partition) match {
      case Some(leaderAndIsr) =>
        val replicasNotInIsr = replicas.filterNot(r => leaderAndIsr.isr.contains(r))
        replicasNotInIsr.isEmpty
      case None => false
    }
  }

  private def moveReassignedPartitionLeaderIfRequired(topicAndPartition: TopicAndPartition,
                                                      reassignedPartitionContext: ReassignedPartitionsContext) {
    val reassignedReplicas = reassignedPartitionContext.newReplicas
    val currentLeader = controllerContext.partitionLeadershipInfo(topicAndPartition).leaderAndIsr.leader
    // change the assigned replica list to just the reassigned replicas in the cache so it gets sent out on the LeaderAndIsr
    // request to the current or new leader. This will prevent it from adding the old replicas to the ISR
    val oldAndNewReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
    controllerContext.partitionReplicaAssignment.put(topicAndPartition, reassignedReplicas)
    if(!reassignedPartitionContext.newReplicas.contains(currentLeader)) {
      info("Leader %s for partition %s being reassigned, ".format(currentLeader, topicAndPartition) +
        "is not in the new list of replicas %s. Re-electing leader".format(reassignedReplicas.mkString(",")))
      // move the leader to one of the alive and caught up new replicas
      partitionStateMachine.handleStateChanges(Set(topicAndPartition), OnlinePartition, reassignedPartitionLeaderSelector)
    } else {
      // check if the leader is alive or not
      controllerContext.liveBrokerIds.contains(currentLeader) match {
        case true =>
          info("Leader %s for partition %s being reassigned, ".format(currentLeader, topicAndPartition) +
            "is already in the new list of replicas %s and is alive".format(reassignedReplicas.mkString(",")))
          // shrink replication factor and update the leader epoch in zookeeper to use on the next LeaderAndIsrRequest
          updateLeaderEpochAndSendRequest(topicAndPartition, oldAndNewReplicas, reassignedReplicas)
        case false =>
          info("Leader %s for partition %s being reassigned, ".format(currentLeader, topicAndPartition) +
            "is already in the new list of replicas %s but is dead".format(reassignedReplicas.mkString(",")))
          partitionStateMachine.handleStateChanges(Set(topicAndPartition), OnlinePartition, reassignedPartitionLeaderSelector)
      }
    }
  }

  private def stopOldReplicasOfReassignedPartition(topicAndPartition: TopicAndPartition,
                                                   reassignedPartitionContext: ReassignedPartitionsContext,
                                                   oldReplicas: Set[Int]) {
    val topic = topicAndPartition.topic
    val partition = topicAndPartition.partition
    // first move the replica to offline state (the controller removes it from the ISR)
    val replicasToBeDeleted = oldReplicas.map(r => PartitionAndReplica(topic, partition, r))
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, OfflineReplica)
    // send stop replica command to the old replicas
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, ReplicaDeletionStarted)
    // TODO: Eventually partition reassignment could use a callback that does retries if deletion failed
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, ReplicaDeletionSuccessful)
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, NonExistentReplica)
  }

  private def updateAssignedReplicasForPartition(topicAndPartition: TopicAndPartition,
                                                 replicas: Seq[Int]) {
    val partitionsAndReplicasForThisTopic = controllerContext.partitionReplicaAssignment.filter(_._1.topic.equals(topicAndPartition.topic))
    partitionsAndReplicasForThisTopic.put(topicAndPartition, replicas)
    updateAssignedReplicasForPartition(topicAndPartition, partitionsAndReplicasForThisTopic)
    info("Updated assigned replicas for partition %s being reassigned to %s ".format(topicAndPartition, replicas.mkString(",")))
    // update the assigned replica list after a successful zookeeper write
    controllerContext.partitionReplicaAssignment.put(topicAndPartition, replicas)
  }

  private def startNewReplicasForReassignedPartition(topicAndPartition: TopicAndPartition,
                                                     reassignedPartitionContext: ReassignedPartitionsContext,
                                                     newReplicas: Set[Int]) {
    // send the start replica request to the brokers in the reassigned replicas list that are not in the assigned
    // replicas list
    newReplicas.foreach { replica =>
      replicaStateMachine.handleStateChanges(Set(new PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition, replica)), NewReplica)
    }
  }

  private def updateLeaderEpochAndSendRequest(topicAndPartition: TopicAndPartition, replicasToReceiveRequest: Seq[Int], newAssignedReplicas: Seq[Int]) {
    brokerRequestBatch.newBatch()
    updateLeaderEpoch(topicAndPartition.topic, topicAndPartition.partition) match {
      case Some(updatedLeaderIsrAndControllerEpoch) =>
        try {
          brokerRequestBatch.addLeaderAndIsrRequestForBrokers(replicasToReceiveRequest, topicAndPartition.topic,
            topicAndPartition.partition, updatedLeaderIsrAndControllerEpoch, newAssignedReplicas)
          brokerRequestBatch.sendRequestsToBrokers(controllerContext.epoch)
        } catch {
          case e : IllegalStateException => {
            // Resign if the controller is in an illegal state
            error("Forcing the controller to resign")
            brokerRequestBatch.clear()
            controllerElector.resign()

            throw e
          }
        }
        stateChangeLogger.trace(("Controller %d epoch %d sent LeaderAndIsr request %s with new assigned replica list %s " +
          "to leader %d for partition being reassigned %s").format(config.brokerId, controllerContext.epoch, updatedLeaderIsrAndControllerEpoch,
          newAssignedReplicas.mkString(","), updatedLeaderIsrAndControllerEpoch.leaderAndIsr.leader, topicAndPartition))
      case None => // fail the reassignment
        stateChangeLogger.error(("Controller %d epoch %d failed to send LeaderAndIsr request with new assigned replica list %s " +
          "to leader for partition being reassigned %s").format(config.brokerId, controllerContext.epoch,
          newAssignedReplicas.mkString(","), topicAndPartition))
    }
  }

  private def registerIsrChangeNotificationListener() = {
    debug("Registering IsrChangeNotificationListener")
    zkUtils.zkClient.subscribeChildChanges(ZkUtils.IsrChangeNotificationPath, isrChangeNotificationListener)
  }

  private def deregisterIsrChangeNotificationListener() = {
    debug("De-registering IsrChangeNotificationListener")
    zkUtils.zkClient.unsubscribeChildChanges(ZkUtils.IsrChangeNotificationPath, isrChangeNotificationListener)
  }

  private def registerReassignedPartitionsListener() = {
    zkUtils.zkClient.subscribeDataChanges(ZkUtils.ReassignPartitionsPath, partitionReassignedListener)
  }

  private def deregisterReassignedPartitionsListener() = {
    zkUtils.zkClient.unsubscribeDataChanges(ZkUtils.ReassignPartitionsPath, partitionReassignedListener)
  }

  private def registerPreferredReplicaElectionListener() {
    zkUtils.zkClient.subscribeDataChanges(ZkUtils.PreferredReplicaLeaderElectionPath, preferredReplicaElectionListener)
  }

  private def deregisterPreferredReplicaElectionListener() {
    zkUtils.zkClient.unsubscribeDataChanges(ZkUtils.PreferredReplicaLeaderElectionPath, preferredReplicaElectionListener)
  }

  private def deregisterReassignedPartitionsIsrChangeListeners() {
    controllerContext.partitionsBeingReassigned.foreach {
      case (topicAndPartition, reassignedPartitionsContext) =>
        val zkPartitionPath = getTopicPartitionLeaderAndIsrPath(topicAndPartition.topic, topicAndPartition.partition)
        zkUtils.zkClient.unsubscribeDataChanges(zkPartitionPath, reassignedPartitionsContext.isrChangeListener)
    }
  }

  /**
    * 读取controller epoch的值并更新
    */
  private def readControllerEpochFromZookeeper() {
    // initialize the controller epoch and zk version by reading from zookeeper
    // 判断“"/controller_epoch"节点是否存在
    if(controllerContext.zkUtils.pathExists(ZkUtils.ControllerEpochPath)) {
      // 获取epochData，返回的是(dataStr, stat)
      val epochData = controllerContext.zkUtils.readData(ZkUtils.ControllerEpochPath)
      // 更新本地记录的epoch和epoch的zk版本
      controllerContext.epoch = epochData._1.toInt
      controllerContext.epochZkVersion = epochData._2.getVersion
      info("Initialized controller epoch to %d and zk version %d".format(controllerContext.epoch, controllerContext.epochZkVersion))
    }
  }

  def removePartitionFromReassignedPartitions(topicAndPartition: TopicAndPartition) {
    if(controllerContext.partitionsBeingReassigned.get(topicAndPartition).isDefined) {
      // stop watching the ISR changes for this partition
      // 取消对这个分区ISR列表的监听
      zkUtils.zkClient.unsubscribeDataChanges(getTopicPartitionLeaderAndIsrPath(topicAndPartition.topic, topicAndPartition.partition),
        controllerContext.partitionsBeingReassigned(topicAndPartition).isrChangeListener)
    }
    // read the current list of reassigned partitions from zookeeper
    // 从zk中读取"/admin/reassign_partitions"节点下重分配的分区列表
    val partitionsBeingReassigned = zkUtils.getPartitionsBeingReassigned()
    // remove this partition from that list
    // 剔除掉当前被删除的topic分区
    val updatedPartitionsBeingReassigned = partitionsBeingReassigned - topicAndPartition
    // write the new list to zookeeper
    // 更新"/admin/reassign_partitions"节点数据
    zkUtils.updatePartitionReassignmentData(updatedPartitionsBeingReassigned.mapValues(_.newReplicas))
    // update the cache. NO-OP if the partition's reassignment was never started
    // 从正在执行重分配的分区中剔除当前分区
    controllerContext.partitionsBeingReassigned.remove(topicAndPartition)
  }

  def updateAssignedReplicasForPartition(topicAndPartition: TopicAndPartition,
                                         newReplicaAssignmentForTopic: Map[TopicAndPartition, Seq[Int]]) {
    try {
      val zkPath = getTopicPath(topicAndPartition.topic)
      val jsonPartitionMap = zkUtils.replicaAssignmentZkData(newReplicaAssignmentForTopic.map(e => (e._1.partition.toString -> e._2)))
      zkUtils.updatePersistentPath(zkPath, jsonPartitionMap)
      debug("Updated path %s with %s for replica assignment".format(zkPath, jsonPartitionMap))
    } catch {
      case e: ZkNoNodeException => throw new IllegalStateException("Topic %s doesn't exist".format(topicAndPartition.topic))
      case e2: Throwable => throw new KafkaException(e2.toString)
    }
  }

  def removePartitionsFromPreferredReplicaElection(partitionsToBeRemoved: Set[TopicAndPartition],
                                                   isTriggeredByAutoRebalance : Boolean) {
    for(partition <- partitionsToBeRemoved) {
      // check the status
      val currentLeader = controllerContext.partitionLeadershipInfo(partition).leaderAndIsr.leader
      val preferredReplica = controllerContext.partitionReplicaAssignment(partition).head
      if(currentLeader == preferredReplica) {
        info("Partition %s completed preferred replica leader election. New leader is %d".format(partition, preferredReplica))
      } else {
        warn("Partition %s failed to complete preferred replica leader election. Leader is %d".format(partition, currentLeader))
      }
    }
    if (!isTriggeredByAutoRebalance)
      zkUtils.deletePath(ZkUtils.PreferredReplicaLeaderElectionPath)
    controllerContext.partitionsUndergoingPreferredReplicaElection --= partitionsToBeRemoved
  }

  /**
   * Send the leader information for selected partitions to selected brokers so that they can correctly respond to
   * metadata requests
   * @param brokers The brokers that the update metadata request should be sent to 接收请求的BrokerId列表
   */
  def sendUpdateMetadataRequest(brokers: Seq[Int], partitions: Set[TopicAndPartition] = Set.empty[TopicAndPartition]) {
    try {
      brokerRequestBatch.newBatch()
      brokerRequestBatch.addUpdateMetadataRequestForBrokers(brokers, partitions)
      brokerRequestBatch.sendRequestsToBrokers(epoch)
    } catch {
      case e : IllegalStateException => {
        // Resign if the controller is in an illegal state
        error("Forcing the controller to resign")
        brokerRequestBatch.clear()
        controllerElector.resign()

        throw e
      }
    }
  }

  /**
   * Removes a given partition replica from the ISR; if it is not the current
   * leader and there are sufficient remaining replicas in ISR.
   * @param topic topic
   * @param partition partition
   * @param replicaId replica Id
   * @return the new leaderAndIsr (with the replica removed if it was present),
   *         or None if leaderAndIsr is empty.
   */
  def removeReplicaFromIsr(topic: String, partition: Int, replicaId: Int): Option[LeaderIsrAndControllerEpoch] = {
    val topicAndPartition = TopicAndPartition(topic, partition)
    debug("Removing replica %d from ISR %s for partition %s.".format(replicaId,
      controllerContext.partitionLeadershipInfo(topicAndPartition).leaderAndIsr.isr.mkString(","), topicAndPartition))
    var finalLeaderIsrAndControllerEpoch: Option[LeaderIsrAndControllerEpoch] = None
    var zkWriteCompleteOrUnnecessary = false
    while (!zkWriteCompleteOrUnnecessary) {
      // refresh leader and isr from zookeeper again
      val leaderIsrAndEpochOpt = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, topic, partition)
      zkWriteCompleteOrUnnecessary = leaderIsrAndEpochOpt match {
        case Some(leaderIsrAndEpoch) => // increment the leader epoch even if the ISR changes
          val leaderAndIsr = leaderIsrAndEpoch.leaderAndIsr
          val controllerEpoch = leaderIsrAndEpoch.controllerEpoch
          if(controllerEpoch > epoch)
            throw new StateChangeFailedException("Leader and isr path written by another controller. This probably" +
              "means the current controller with epoch %d went through a soft failure and another ".format(epoch) +
              "controller was elected with epoch %d. Aborting state change by this controller".format(controllerEpoch))
          if (leaderAndIsr.isr.contains(replicaId)) {
            // if the replica to be removed from the ISR is also the leader, set the new leader value to -1
            val newLeader = if (replicaId == leaderAndIsr.leader) LeaderAndIsr.NoLeader else leaderAndIsr.leader
            var newIsr = leaderAndIsr.isr.filter(b => b != replicaId)

            // if the replica to be removed from the ISR is the last surviving member of the ISR and unclean leader election
            // is disallowed for the corresponding topic, then we must preserve the ISR membership so that the replica can
            // eventually be restored as the leader.
            if (newIsr.isEmpty && !LogConfig.fromProps(config.originals, AdminUtils.fetchEntityConfig(zkUtils,
              ConfigType.Topic, topicAndPartition.topic)).uncleanLeaderElectionEnable) {
              info("Retaining last ISR %d of partition %s since unclean leader election is disabled".format(replicaId, topicAndPartition))
              newIsr = leaderAndIsr.isr
            }

            val newLeaderAndIsr = new LeaderAndIsr(newLeader, leaderAndIsr.leaderEpoch + 1,
              newIsr, leaderAndIsr.zkVersion + 1)
            // update the new leadership decision in zookeeper or retry
            val (updateSucceeded, newVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, topic, partition,
              newLeaderAndIsr, epoch, leaderAndIsr.zkVersion)

            newLeaderAndIsr.zkVersion = newVersion
            finalLeaderIsrAndControllerEpoch = Some(LeaderIsrAndControllerEpoch(newLeaderAndIsr, epoch))
            controllerContext.partitionLeadershipInfo.put(topicAndPartition, finalLeaderIsrAndControllerEpoch.get)
            if (updateSucceeded)
              info("New leader and ISR for partition %s is %s".format(topicAndPartition, newLeaderAndIsr.toString()))
            updateSucceeded
          } else {
            warn("Cannot remove replica %d from ISR of partition %s since it is not in the ISR. Leader = %d ; ISR = %s"
                 .format(replicaId, topicAndPartition, leaderAndIsr.leader, leaderAndIsr.isr))
            finalLeaderIsrAndControllerEpoch = Some(LeaderIsrAndControllerEpoch(leaderAndIsr, epoch))
            controllerContext.partitionLeadershipInfo.put(topicAndPartition, finalLeaderIsrAndControllerEpoch.get)
            true
          }
        case None =>
          warn("Cannot remove replica %d from ISR of %s - leaderAndIsr is empty.".format(replicaId, topicAndPartition))
          true
      }
    }
    finalLeaderIsrAndControllerEpoch
  }

  /**
   * Does not change leader or isr, but just increments the leader epoch
   * @param topic topic
   * @param partition partition
   * @return the new leaderAndIsr with an incremented leader epoch, or None if leaderAndIsr is empty.
   */
  private def updateLeaderEpoch(topic: String, partition: Int): Option[LeaderIsrAndControllerEpoch] = {
    val topicAndPartition = TopicAndPartition(topic, partition)
    debug("Updating leader epoch for partition %s.".format(topicAndPartition))
    var finalLeaderIsrAndControllerEpoch: Option[LeaderIsrAndControllerEpoch] = None
    var zkWriteCompleteOrUnnecessary = false
    while (!zkWriteCompleteOrUnnecessary) {
      // refresh leader and isr from zookeeper again
      val leaderIsrAndEpochOpt = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, topic, partition)
      zkWriteCompleteOrUnnecessary = leaderIsrAndEpochOpt match {
        case Some(leaderIsrAndEpoch) =>
          val leaderAndIsr = leaderIsrAndEpoch.leaderAndIsr
          val controllerEpoch = leaderIsrAndEpoch.controllerEpoch
          if(controllerEpoch > epoch)
            throw new StateChangeFailedException("Leader and isr path written by another controller. This probably" +
              "means the current controller with epoch %d went through a soft failure and another ".format(epoch) +
              "controller was elected with epoch %d. Aborting state change by this controller".format(controllerEpoch))
          // increment the leader epoch even if there are no leader or isr changes to allow the leader to cache the expanded
          // assigned replica list
          val newLeaderAndIsr = new LeaderAndIsr(leaderAndIsr.leader, leaderAndIsr.leaderEpoch + 1,
                                                 leaderAndIsr.isr, leaderAndIsr.zkVersion + 1)
          // update the new leadership decision in zookeeper or retry
          val (updateSucceeded, newVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, topic,
            partition, newLeaderAndIsr, epoch, leaderAndIsr.zkVersion)

          newLeaderAndIsr.zkVersion = newVersion
          finalLeaderIsrAndControllerEpoch = Some(LeaderIsrAndControllerEpoch(newLeaderAndIsr, epoch))
          if (updateSucceeded)
            info("Updated leader epoch for partition %s to %d".format(topicAndPartition, newLeaderAndIsr.leaderEpoch))
          updateSucceeded
        case None =>
          throw new IllegalStateException(("Cannot update leader epoch for partition %s as leaderAndIsr path is empty. " +
            "This could mean we somehow tried to reassign a partition that doesn't exist").format(topicAndPartition))
          true
      }
    }
    finalLeaderIsrAndControllerEpoch
  }

  /**
    * Session过期监听器
    */
  class SessionExpirationListener() extends IZkStateListener with Logging {
    this.logIdent = "[SessionExpirationListener on " + config.brokerId + "], "
    @throws(classOf[Exception])
    def handleStateChanged(state: KeeperState) {
      // do nothing, since zkclient will do reconnect for us.
    }

    /**
     * Called after the zookeeper session has expired and a new session has been created. You would have to re-create
     * any ephemeral nodes here.
      * 在zookeeper会话过期并创建了新的会话后调用该方法
     *
     * @throws Exception
     *             On any error.
     */
    @throws(classOf[Exception])
    def handleNewSession() {
      info("ZK expired; shut down all controller components and try to re-elect")
      inLock(controllerContext.controllerLock) {
        // 先注销一些已经注册的监听器，关闭资源
        onControllerResignation()
        // 重新尝试选举成controller leader
        controllerElector.elect
      }
    }

    override def handleSessionEstablishmentError(error: Throwable): Unit = {
      //no-op handleSessionEstablishmentError in KafkaHealthCheck should handle this error in its handleSessionEstablishmentError
    }
  }

  /**
    * 定时对所有topic-partition的leader检查
    * 确定是否执行leader的重新选举
    * leader一般情况下都是ar列表的第一个
    */
  private def checkAndTriggerPartitionRebalance(): Unit = {
    if (isActive()) {
      trace("checking need to trigger partition rebalance")
      // get all the active brokers
      // 获取所有的broker
      var preferredReplicasForTopicsByBrokers: Map[Int, Map[TopicAndPartition, Seq[Int]]] = null
      inLock(controllerContext.controllerLock) {
        preferredReplicasForTopicsByBrokers =
          // 遍历每一个topic-partition过滤掉被删除的topic,之后根据ar集合中的leader进行分组，leader也是broker
          // <borkerID,<topic-partiton,ar列表>>
          controllerContext.partitionReplicaAssignment.filterNot(p => deleteTopicManager.isTopicQueuedUpForDeletion(p._1.topic)).groupBy {
            case(topicAndPartition, assignedReplicas) => assignedReplicas.head
          }
      }
      debug("preferred replicas by broker " + preferredReplicasForTopicsByBrokers)
      // for each broker, check if a preferred replica election needs to be triggered
      // 遍历<borkerID,<topic-partition,ar列表>>集合
      preferredReplicasForTopicsByBrokers.foreach {
        // 遍历出来的key-value
        case(leaderBroker, topicAndPartitionsForBroker) => {
          var imbalanceRatio: Double = 0
          var topicsNotInPreferredReplica: Map[TopicAndPartition, Seq[Int]] = null
          inLock(controllerContext.controllerLock) {
            topicsNotInPreferredReplica =
              // 遍历<topic-partition,ar列表>集合中的value
              topicAndPartitionsForBroker.filter {
                // 遍历出来的key-value
                case(topicPartition, replicas) => {
                  // 过滤出topic-partition的ar列表中第一个副本和当前topic-partition的leader不是同一个的
                  controllerContext.partitionLeadershipInfo.contains(topicPartition) &&
                  controllerContext.partitionLeadershipInfo(topicPartition).leaderAndIsr.leader != leaderBroker
                }
              }
            debug("topics not in preferred replica " + topicsNotInPreferredReplica)
            // 当前broker的topic-partition的ar列表大小
            val totalTopicPartitionsForBroker = topicAndPartitionsForBroker.size
            // leader不是ar副本集第一个的topic-partition的大小
            val totalTopicPartitionsNotLedByBroker = topicsNotInPreferredReplica.size
            // 计算占比
            imbalanceRatio = totalTopicPartitionsNotLedByBroker.toDouble / totalTopicPartitionsForBroker
            trace("leader imbalance ratio for broker %d is %f".format(leaderBroker, imbalanceRatio))
          }
          // check ratio and if greater than desired ratio, trigger a rebalance for the topic partitions
          // that need to be on this broker
          // 如果占比超过"leader.imbalance.per.broker.percentage"默认10
          // 触发leader不是第一个的topic-partition的重分配
          if (imbalanceRatio > (config.leaderImbalancePerBrokerPercentage.toDouble / 100)) {
            // 遍历需要重分配的topic-partition
            topicsNotInPreferredReplica.foreach {
              case(topicPartition, replicas) => {
                inLock(controllerContext.controllerLock) {
                  // do this check only if the broker is live and there are no partitions being reassigned currently
                  // and preferred replica election is not in progress
                  // 确保broker存活
                  // 没有分区正在重新分配
                  // 没有进行preferredreplica选举
                  // 没有分区将被删除
                  if (controllerContext.liveBrokerIds.contains(leaderBroker) &&
                      controllerContext.partitionsBeingReassigned.size == 0 &&
                      controllerContext.partitionsUndergoingPreferredReplicaElection.size == 0 &&
                      !deleteTopicManager.isTopicQueuedUpForDeletion(topicPartition.topic) &&
                      controllerContext.allTopics.contains(topicPartition.topic)) {
                    // 启动leader重新选举
                    onPreferredReplicaElection(Set(topicPartition), true)
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}

/**
 * Starts the partition reassignment process unless -
 * 1. Partition previously existed
 * 2. New replicas are the same as existing replicas
 * 3. Any replica in the new set of replicas are dead
 * If any of the above conditions are satisfied, it logs an error and removes the partition from list of reassigned
 * partitions.
 */
class PartitionsReassignedListener(controller: KafkaController) extends IZkDataListener with Logging {
  this.logIdent = "[PartitionsReassignedListener on " + controller.config.brokerId + "]: "
  val zkUtils = controller.controllerContext.zkUtils
  val controllerContext = controller.controllerContext

  /**
   * Invoked when some partitions are reassigned by the admin command
    * 当管理命令重新分配一些分区时调用
   * @throws Exception On any error.
   */
  @throws(classOf[Exception])
  def handleDataChange(dataPath: String, data: Object) {
    debug("Partitions reassigned listener fired for path %s. Record partitions to be reassigned %s"
      .format(dataPath, data))
    // 获取需要重分配的分区数据
    val partitionsReassignmentData = zkUtils.parsePartitionReassignmentData(data.toString)
    // 过滤掉正在重分配的数据
    val partitionsToBeReassigned = inLock(controllerContext.controllerLock) {
      partitionsReassignmentData.filterNot(p => controllerContext.partitionsBeingReassigned.contains(p._1))
    }
    // 遍历需要被重分配的分区
    partitionsToBeReassigned.foreach { partitionToBeReassigned =>
      inLock(controllerContext.controllerLock) {
        // 如果要执行副本重分配的topic已经被删除
        if(controller.deleteTopicManager.isTopicQueuedUpForDeletion(partitionToBeReassigned._1.topic)) {
          error("Skipping reassignment of partition %s for topic %s since it is currently being deleted"
            .format(partitionToBeReassigned._1, partitionToBeReassigned._1.topic))
          // 更新/admin/reassign_partitions"节点数据提成已经删除的topic的副本
          controller.removePartitionFromReassignedPartitions(partitionToBeReassigned._1)
        } else {
          // 开始进行分区重分配
          val context = new ReassignedPartitionsContext(partitionToBeReassigned._2)
          controller.initiateReassignReplicasForTopicPartition(partitionToBeReassigned._1, context)
        }
      }
    }
  }

  /**
   * Called when the leader information stored in zookeeper has been delete. Try to elect as the leader
   * @throws Exception
   *             On any error.
   */
  @throws(classOf[Exception])
  def handleDataDeleted(dataPath: String) {
  }
}

// 监听ISR列表的变化，节点路径：/brokers/topics/{topic}/partitions/{partitionId}/state节点
class ReassignedPartitionsIsrChangeListener(controller: KafkaController, topic: String, partition: Int,
                                            reassignedReplicas: Set[Int])
  extends IZkDataListener with Logging {
  this.logIdent = "[ReassignedPartitionsIsrChangeListener on controller " + controller.config.brokerId + "]: "
  val zkUtils = controller.controllerContext.zkUtils
  val controllerContext = controller.controllerContext

  /**
   * Invoked when some partitions need to move leader to preferred replica
   * @throws Exception On any error.
   */
  @throws(classOf[Exception])
  def handleDataChange(dataPath: String, data: Object) {
    inLock(controllerContext.controllerLock) {
      debug("Reassigned partitions isr change listener fired for path %s with children %s".format(dataPath, data))
      val topicAndPartition = TopicAndPartition(topic, partition)
      try {
        // check if this partition is still being reassigned or not
        // 检查这个分区是否仍在重新分配中
        controllerContext.partitionsBeingReassigned.get(topicAndPartition) match {
          case Some(reassignedPartitionContext) =>
            // need to re-read leader and isr from zookeeper since the zkclient callback doesn't return the Stat object
            // 从zk获取最新的leader和ISR信息
            val newLeaderAndIsrOpt = zkUtils.getLeaderAndIsrForPartition(topic, partition)
            newLeaderAndIsrOpt match {
                /// 加入了新副本
              case Some(leaderAndIsr) => // check if new replicas have joined ISR
                val caughtUpReplicas = reassignedReplicas & leaderAndIsr.isr.toSet
                // 新分配的副本已经全部在ISR中了
                if(caughtUpReplicas == reassignedReplicas) {
                  // resume the partition reassignment process
                  info("%d/%d replicas have caught up with the leader for partition %s being reassigned."
                    .format(caughtUpReplicas.size, reassignedReplicas.size, topicAndPartition) +
                    "Resuming partition reassignment")
                  // 执行分区重分配
                  controller.onPartitionReassignment(topicAndPartition, reassignedPartitionContext)
                }
                else {
                  info("%d/%d replicas have caught up with the leader for partition %s being reassigned."
                    .format(caughtUpReplicas.size, reassignedReplicas.size, topicAndPartition) +
                    "Replica(s) %s still need to catch up".format((reassignedReplicas -- leaderAndIsr.isr.toSet).mkString(",")))
                }
              case None => error("Error handling reassignment of partition %s to replicas %s as it was never created"
                .format(topicAndPartition, reassignedReplicas.mkString(",")))
            }
          case None =>
        }
      } catch {
        case e: Throwable => error("Error while handling partition reassignment", e)
      }
    }
  }

  /**
   * @throws Exception
   *             On any error.
   */
  @throws(classOf[Exception])
  def handleDataDeleted(dataPath: String) {
  }
}

/**
 * Called when leader intimates of isr change
 * @param controller
  * 监听"/isr_change_notification",用来更新各个broker的缓存
 */
class IsrChangeNotificationListener(controller: KafkaController) extends IZkChildListener with Logging {

  override def handleChildChange(parentPath: String, currentChildren: util.List[String]): Unit = {
    import scala.collection.JavaConverters._

    inLock(controller.controllerContext.controllerLock) {
      debug("[IsrChangeNotificationListener] Fired!!!")
      // 获取"/isr_change_notification"的所有子节点
      // 如：/isr_change_notification/isr_change_0000000013
      val childrenAsScala: mutable.Buffer[String] = currentChildren.asScala
      try {
        // 转换为topic-partiton集合
        val topicAndPartitions: immutable.Set[TopicAndPartition] = childrenAsScala.map(x => getTopicAndPartition(x)).flatten.toSet
        if (topicAndPartitions.nonEmpty) {
          // 更新controllerContext.partitionLeadershipInfo
          controller.updateLeaderAndIsrCache(topicAndPartitions)
          // 发送UpdateMetadataRequest到每个broker上
          processUpdateNotifications(topicAndPartitions)
        }
      } finally {
        // delete processed children
        // 删除添加的节点 /isr_change_notification/isr_change_0000000013
        childrenAsScala.map(x => controller.controllerContext.zkUtils.deletePath(
          ZkUtils.IsrChangeNotificationPath + "/" + x))
      }
    }
  }

  private def processUpdateNotifications(topicAndPartitions: immutable.Set[TopicAndPartition]) {
    val liveBrokers: Seq[Int] = controller.controllerContext.liveOrShuttingDownBrokerIds.toSeq
    debug("Sending MetadataRequest to Brokers:" + liveBrokers + " for TopicAndPartitions:" + topicAndPartitions)
    controller.sendUpdateMetadataRequest(liveBrokers, topicAndPartitions)
  }

  private def getTopicAndPartition(child: String): Set[TopicAndPartition] = {
    //
    val changeZnode: String = ZkUtils.IsrChangeNotificationPath + "/" + child
    val (jsonOpt, stat) = controller.controllerContext.zkUtils.readDataMaybeNull(changeZnode)
    if (jsonOpt.isDefined) {
      val json = Json.parseFull(jsonOpt.get)

      json match {
        case Some(m) =>
          val topicAndPartitions: mutable.Set[TopicAndPartition] = new mutable.HashSet[TopicAndPartition]()
          val isrChanges = m.asInstanceOf[Map[String, Any]]
          val topicAndPartitionList = isrChanges("partitions").asInstanceOf[List[Any]]
          topicAndPartitionList.foreach {
            case tp =>
              val topicAndPartition = tp.asInstanceOf[Map[String, Any]]
              val topic = topicAndPartition("topic").asInstanceOf[String]
              val partition = topicAndPartition("partition").asInstanceOf[Int]
              topicAndPartitions += TopicAndPartition(topic, partition)
          }
          topicAndPartitions
        case None =>
          error("Invalid topic and partition JSON: " + jsonOpt.get + " in ZK: " + changeZnode)
          Set.empty
      }
    } else {
      Set.empty
    }
  }
}

object IsrChangeNotificationListener {
  val version: Long = 1L
}

/**
 * Starts the preferred replica leader election for the list of partitions specified under
 * /admin/preferred_replica_election -
 */
class PreferredReplicaElectionListener(controller: KafkaController) extends IZkDataListener with Logging {
  this.logIdent = "[PreferredReplicaElectionListener on " + controller.config.brokerId + "]: "
  val zkUtils = controller.controllerContext.zkUtils
  val controllerContext = controller.controllerContext

  /**
   * Invoked when some partitions are reassigned by the admin command
   * @throws Exception On any error.
   */
  @throws(classOf[Exception])
  def handleDataChange(dataPath: String, data: Object) {
    debug("Preferred replica election listener fired for path %s. Record partitions to undergo preferred replica election %s"
            .format(dataPath, data.toString))
    inLock(controllerContext.controllerLock) {
      // 需要进行分区leader优化的副本
      val partitionsForPreferredReplicaElection = PreferredReplicaLeaderElectionCommand.parsePreferredReplicaElectionData(data.toString)
      if(controllerContext.partitionsUndergoingPreferredReplicaElection.size > 0)
        info("These partitions are already undergoing preferred replica election: %s"
          .format(controllerContext.partitionsUndergoingPreferredReplicaElection.mkString(",")))
      // 新增需要进行leader优化的副本
      val partitions = partitionsForPreferredReplicaElection -- controllerContext.partitionsUndergoingPreferredReplicaElection
      // 从新增的需要leader优化的副本中过滤掉主题等待被删除的
      val partitionsForTopicsToBeDeleted = partitions.filter(p => controller.deleteTopicManager.isTopicQueuedUpForDeletion(p.topic))
      if(partitionsForTopicsToBeDeleted.size > 0) {
        error("Skipping preferred replica election for partitions %s since the respective topics are being deleted"
          .format(partitionsForTopicsToBeDeleted))
      }
      // 将新增的需要leader优化的分区加入到controllerContext.partitionsUndergoingPreferredReplicaElection
      // 然后如果是被删除的暂停删除
      // 执行分区leader重选举
      controller.onPreferredReplicaElection(partitions -- partitionsForTopicsToBeDeleted)
    }
  }

  /**
   * @throws Exception
   *             On any error.
   */
  @throws(classOf[Exception])
  def handleDataDeleted(dataPath: String) {
  }
}

case class ReassignedPartitionsContext(var newReplicas: Seq[Int] = Seq.empty,
                                       var isrChangeListener: ReassignedPartitionsIsrChangeListener = null)

case class PartitionAndReplica(topic: String, partition: Int, replica: Int) {
  override def toString(): String = {
    "[Topic=%s,Partition=%d,Replica=%d]".format(topic, partition, replica)
  }
}

case class LeaderIsrAndControllerEpoch(leaderAndIsr: LeaderAndIsr, controllerEpoch: Int) {
  override def toString(): String = {
    val leaderAndIsrInfo = new StringBuilder
    leaderAndIsrInfo.append("(Leader:" + leaderAndIsr.leader)
    leaderAndIsrInfo.append(",ISR:" + leaderAndIsr.isr.mkString(","))
    leaderAndIsrInfo.append(",LeaderEpoch:" + leaderAndIsr.leaderEpoch)
    leaderAndIsrInfo.append(",ControllerEpoch:" + controllerEpoch + ")")
    leaderAndIsrInfo.toString()
  }
}

object ControllerStats extends KafkaMetricsGroup {

  private val _uncleanLeaderElectionRate = newMeter("UncleanLeaderElectionsPerSec", "elections", TimeUnit.SECONDS)
  private val _leaderElectionTimer = new KafkaTimer(newTimer("LeaderElectionRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS))

  // KafkaServer needs to initialize controller metrics during startup. We perform initialization
  // through method calls to avoid Scala compiler warnings.
  def uncleanLeaderElectionRate: Meter = _uncleanLeaderElectionRate

  def leaderElectionTimer: KafkaTimer = _leaderElectionTimer
}
