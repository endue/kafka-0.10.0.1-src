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

import java.net.{SocketTimeoutException}
import java.util

import kafka.admin._
import kafka.api.KAFKA_0_9_0
import kafka.log.LogConfig
import kafka.log.CleanerConfig
import kafka.log.LogManager
import java.util.concurrent._
import atomic.{AtomicInteger, AtomicBoolean}
import java.io.{IOException, File}

import kafka.security.auth.Authorizer
import kafka.utils._
import org.apache.kafka.clients.{ManualMetadataUpdater, ClientRequest, NetworkClient}
import org.apache.kafka.common.Node
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.network.{LoginType, Selectable, ChannelBuilders, NetworkReceive, Selector, Mode}
import org.apache.kafka.common.protocol.{Errors, ApiKeys, SecurityProtocol}
import org.apache.kafka.common.metrics.{JmxReporter, Metrics}
import org.apache.kafka.common.requests.{ControlledShutdownResponse, ControlledShutdownRequest, RequestSend}
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.utils.AppInfoParser

import scala.collection.mutable
import scala.collection.JavaConverters._
import org.I0Itec.zkclient.ZkClient
import kafka.controller.{ControllerStats, KafkaController}
import kafka.cluster.{EndPoint, Broker}
import kafka.common.{InconsistentBrokerIdException, GenerateBrokerIdException}
import kafka.network.{BlockingChannel, SocketServer}
import kafka.metrics.KafkaMetricsGroup
import com.yammer.metrics.core.Gauge
import kafka.coordinator.GroupCoordinator

object KafkaServer {
  // Copy the subset of properties that are relevant to Logs
  // I'm listing out individual properties here since the names are slightly different in each Config class...
  private[kafka] def copyKafkaConfigToLog(kafkaConfig: KafkaConfig): java.util.Map[String, Object] = {
    val logProps = new util.HashMap[String, Object]()
    logProps.put(LogConfig.SegmentBytesProp, kafkaConfig.logSegmentBytes)
    logProps.put(LogConfig.SegmentMsProp, kafkaConfig.logRollTimeMillis)
    logProps.put(LogConfig.SegmentJitterMsProp, kafkaConfig.logRollTimeJitterMillis)
    logProps.put(LogConfig.SegmentIndexBytesProp, kafkaConfig.logIndexSizeMaxBytes)
    logProps.put(LogConfig.FlushMessagesProp, kafkaConfig.logFlushIntervalMessages)
    logProps.put(LogConfig.FlushMsProp, kafkaConfig.logFlushIntervalMs)
    logProps.put(LogConfig.RetentionBytesProp, kafkaConfig.logRetentionBytes)
    logProps.put(LogConfig.RetentionMsProp, kafkaConfig.logRetentionTimeMillis: java.lang.Long)
    logProps.put(LogConfig.MaxMessageBytesProp, kafkaConfig.messageMaxBytes)
    logProps.put(LogConfig.IndexIntervalBytesProp, kafkaConfig.logIndexIntervalBytes)
    logProps.put(LogConfig.DeleteRetentionMsProp, kafkaConfig.logCleanerDeleteRetentionMs)
    logProps.put(LogConfig.FileDeleteDelayMsProp, kafkaConfig.logDeleteDelayMs)
    logProps.put(LogConfig.MinCleanableDirtyRatioProp, kafkaConfig.logCleanerMinCleanRatio)
    logProps.put(LogConfig.CleanupPolicyProp, kafkaConfig.logCleanupPolicy)
    logProps.put(LogConfig.MinInSyncReplicasProp, kafkaConfig.minInSyncReplicas)
    logProps.put(LogConfig.CompressionTypeProp, kafkaConfig.compressionType)
    logProps.put(LogConfig.UncleanLeaderElectionEnableProp, kafkaConfig.uncleanLeaderElectionEnable)
    logProps.put(LogConfig.PreAllocateEnableProp, kafkaConfig.logPreAllocateEnable)
    logProps.put(LogConfig.MessageFormatVersionProp, kafkaConfig.logMessageFormatVersion.version)
    logProps.put(LogConfig.MessageTimestampTypeProp, kafkaConfig.logMessageTimestampType.name)
    logProps.put(LogConfig.MessageTimestampDifferenceMaxMsProp, kafkaConfig.logMessageTimestampDifferenceMaxMs)
    logProps
  }
}



/**
 * Represents the lifecycle of a single Kafka broker. Handles all functionality required
 * to start up and shutdown a single Kafka node.
 */
class KafkaServer(val config: KafkaConfig, time: Time = SystemTime, threadNamePrefix: Option[String] = None) extends Logging with KafkaMetricsGroup {
  // kafkaServer是否完成启动标识符
  private val startupComplete = new AtomicBoolean(false)
  // kafkaServer是否关闭标识符
  private val isShuttingDown = new AtomicBoolean(false)
  // kafkaServer是否正在启动标识符
  private val isStartingUp = new AtomicBoolean(false)

  // 服务启动和关闭时使用
  private var shutdownLatch = new CountDownLatch(1)

  private val jmxPrefix: String = "kafka.server"
  private val reporters: java.util.List[MetricsReporter] = config.metricReporterClasses
  reporters.add(new JmxReporter(jmxPrefix))

  // This exists because the Metrics package from clients has its own Time implementation.
  // SocketServer/Quotas (which uses client libraries) have to use the client Time objects without having to convert all of Kafka to use them
  // Eventually, we want to merge the Time objects in core and clients
  private implicit val kafkaMetricsTime: org.apache.kafka.common.utils.Time = new org.apache.kafka.common.utils.SystemTime()
  var metrics: Metrics = null

  private val metricConfig: MetricConfig = new MetricConfig()
    .samples(config.metricNumSamples)
    .timeWindow(config.metricSampleWindowMs, TimeUnit.MILLISECONDS)
  // kafkaServer的状态，当成为kafkaController后为RunningAsController
  val brokerState: BrokerState = new BrokerState

  var apis: KafkaApis = null
  var authorizer: Option[Authorizer] = None
  var socketServer: SocketServer = null
  // 创建处理请求线程池池，将KafkaApis传递进去，默认8个线程
  var requestHandlerPool: KafkaRequestHandlerPool = null
  // 日志管理组件
  var logManager: LogManager = null
  // 副本管理组件
  var replicaManager: ReplicaManager = null

  var dynamicConfigHandlers: Map[String, ConfigHandler] = null
  var dynamicConfigManager: DynamicConfigManager = null
  // 消费者管理组件
  var groupCoordinator: GroupCoordinator = null
  // kafkaController负责集群间通信组件
  var kafkaController: KafkaController = null

  // 执行定时任务线程池
  val kafkaScheduler = new KafkaScheduler(config.backgroundThreads)// 默认10
  // kafka服务健康检查
  var kafkaHealthcheck: KafkaHealthcheck = null
  // 缓存的元数据
  val metadataCache: MetadataCache = new MetadataCache(config.brokerId)
  // ZkUtils
  var zkUtils: ZkUtils = null
  val correlationId: AtomicInteger = new AtomicInteger(0)
  val brokerMetaPropsFile = "meta.properties"
  val brokerMetadataCheckpoints = config.logDirs.map(logDir => (logDir, new BrokerMetadataCheckpoint(new File(logDir + File.separator +brokerMetaPropsFile)))).toMap

  newGauge(
    "BrokerState",
    new Gauge[Int] {
      def value = brokerState.currentState
    }
  )

  newGauge(
    "yammer-metrics-count",
    new Gauge[Int] {
      def value = {
        com.yammer.metrics.Metrics.defaultRegistry().allMetrics().size()
      }
    }
  )

  /**
   * Start up API for bringing up a single instance of the Kafka server.
   * Instantiates the LogManager, the SocketServer and the request handlers - KafkaRequestHandlers
   * 启动kafkaServer
   */
  def startup() {
    try {
      info("starting")
      // 校验是否关闭
      if(isShuttingDown.get)
        throw new IllegalStateException("Kafka server is still shutting down, cannot re-start!")
      // 校验是否完成启动
      if(startupComplete.get)
        return

      val canStartup = isStartingUp.compareAndSet(false, true)
      if (canStartup) {
        metrics = new Metrics(metricConfig, reporters, kafkaMetricsTime, true)
        // 更新kafkaServer状态
        brokerState.newState(Starting)

        /* start scheduler */
        // 定时任务线程池，用来执行定时任务
        kafkaScheduler.startup()

        /* setup zookeeper */
        // 初始化zk工具类并创建了一些永久节点
        zkUtils = initZk()

        /* start log manager */
        // 创建LogManager
        logManager = createLogManager(zkUtils.zkClient, brokerState)
        // 启动LogManager相关定时任务
        logManager.startup()

        /* generate brokerId */
        // 生成brokerID
        config.brokerId =  getBrokerId
        this.logIdent = "[Kafka Server " + config.brokerId + "], "
        // 创建socker处理请求,这里同时创建Acceptor和Processor
        socketServer = new SocketServer(config, metrics, kafkaMetricsTime)
        socketServer.startup()

        /* start replica manager */
        // 启动副本管理任务
        // 定时更新ISR和HW
        // 定时广播ISR列表到zk
        replicaManager = new ReplicaManager(config, metrics, time, kafkaMetricsTime, zkUtils, kafkaScheduler, logManager,
          isShuttingDown)
        replicaManager.startup()

        /* start kafka controller */
        // 启动kafka controller
        kafkaController = new KafkaController(config, zkUtils, brokerState, kafkaMetricsTime, metrics, threadNamePrefix)
        kafkaController.startup()

        /* start group coordinator */
        // 消费者相关
        groupCoordinator = GroupCoordinator(config, zkUtils, replicaManager, kafkaMetricsTime)
        groupCoordinator.startup()

        /* Get the authorizer and initialize it if one is specified.*/
        // 认证授权相关
        authorizer = Option(config.authorizerClassName).filter(_.nonEmpty).map { authorizerClassName =>
          val authZ = CoreUtils.createObject[Authorizer](authorizerClassName)
          authZ.configure(config.originals())
          authZ
        }

        /* start processing requests */
        // KafkaApis封装了处理各种请求的业务逻辑
        apis = new KafkaApis(socketServer.requestChannel, replicaManager, groupCoordinator,
          kafkaController, zkUtils, config.brokerId, config, metadataCache, metrics, authorizer)
        // 创建处理请求线程池池，将KafkaApis传递进去，num.io.threads默认8个线程
        requestHandlerPool = new KafkaRequestHandlerPool(config.brokerId, socketServer.requestChannel, apis, config.numIoThreads)
        // 设置broker状态为RunningAsBroker
        brokerState.newState(RunningAsBroker)

        Mx4jLoader.maybeLoad()

        /* start dynamic config manager */
        // 加载动态配置管理器
        dynamicConfigHandlers = Map[String, ConfigHandler](ConfigType.Topic -> new TopicConfigHandler(logManager, config),
                                                           ConfigType.Client -> new ClientIdConfigHandler(apis.quotaManagers))

        // Apply all existing client configs to the ClientIdConfigHandler to bootstrap the overrides
        // TODO: Move this logic to DynamicConfigManager
        AdminUtils.fetchAllEntityConfigs(zkUtils, ConfigType.Client).foreach {
          case (clientId, properties) => dynamicConfigHandlers(ConfigType.Client).processConfigChanges(clientId, properties)
        }

        // Create the config manager. start listening to notifications
        // 启动动态配置管理器
        dynamicConfigManager = new DynamicConfigManager(zkUtils, dynamicConfigHandlers)
        dynamicConfigManager.startup()

        /* tell everyone we are alive */
        val listeners = config.advertisedListeners.map {case(protocol, endpoint) =>
          if (endpoint.port == 0)
            (protocol, EndPoint(endpoint.host, socketServer.boundPort(protocol), endpoint.protocolType))
          else
            (protocol, endpoint)
        }
        // 启动健康检查，这里传入了config.rack为机架信息，在创建topic的时候会使用到
        kafkaHealthcheck = new KafkaHealthcheck(config.brokerId, listeners, zkUtils, config.rack,
          config.interBrokerProtocolVersion)// inter.broker.protocol.version
        kafkaHealthcheck.startup()

        // Now that the broker id is successfully registered via KafkaHealthcheck, checkpoint it
        // 写入检查点信息到meta.properties文件中,内容大体如下：
        // #
        // #Sun Jan 17 16:14:47 CST 2021
        // cluster.id=6TTT9G81Tp-ljtt5mYe3Pw
        // version=0
        // broker.id=2
        checkpointBrokerId(config.brokerId)

        /* register broker metrics */
        registerStats()

        shutdownLatch = new CountDownLatch(1)
        startupComplete.set(true)
        isStartingUp.set(false)
        AppInfoParser.registerAppInfo(jmxPrefix, config.brokerId.toString)
        info("started")
      }
    }
    catch {
      case e: Throwable =>
        fatal("Fatal error during KafkaServer startup. Prepare to shutdown", e)
        isStartingUp.set(false)
        shutdown()
        throw e
    }
  }

  // 初始化ZkUtils并创建一个永久的根路径chroot
  private def initZk(): ZkUtils = {
    // config.zkConnect 取的 zookeeper.connect的配置值
    info("Connecting to zookeeper on " + config.zkConnect)
    // 获取根目录
    val chroot = {
      if (config.zkConnect.indexOf("/") > 0)
        config.zkConnect.substring(config.zkConnect.indexOf("/"))
      else
        ""
    }

    val secureAclsEnabled = JaasUtils.isZkSecurityEnabled() && config.zkEnableSecureAcls

    if(config.zkEnableSecureAcls && !secureAclsEnabled) {
      throw new java.lang.SecurityException("zkEnableSecureAcls is true, but the verification of the JAAS login file failed.")
    }
    if (chroot.length > 1) {
      val zkConnForChrootCreation = config.zkConnect.substring(0, config.zkConnect.indexOf("/"))
      // 初始化ZkUtils
      val zkClientForChrootCreation = ZkUtils(zkConnForChrootCreation,
                                              config.zkSessionTimeoutMs,
                                              config.zkConnectionTimeoutMs,
                                              secureAclsEnabled)
      // 创建一个永远的根路径chroot
      zkClientForChrootCreation.makeSurePersistentPathExists(chroot)
      info("Created zookeeper path " + chroot)
      zkClientForChrootCreation.zkClient.close()
    }
    // 创建一个ZkUtils
    val zkUtils = ZkUtils(config.zkConnect,
                          config.zkSessionTimeoutMs,
                          config.zkConnectionTimeoutMs,
                          secureAclsEnabled)
    // 创建一些永久公共必要的路径
    zkUtils.setupCommonPaths()
    zkUtils
  }


  /**
   *  Forces some dynamic jmx beans to be registered on server startup.
   */
  private def registerStats() {
    BrokerTopicStats.getBrokerAllTopicsStats()
    ControllerStats.uncleanLeaderElectionRate
    ControllerStats.leaderElectionTimer
  }

  /**
   *  Performs controlled shutdown
   */
  private def controlledShutdown() {

    def node(broker: Broker): Node = {
      val brokerEndPoint = broker.getBrokerEndPoint(config.interBrokerSecurityProtocol)
      new Node(brokerEndPoint.id, brokerEndPoint.host, brokerEndPoint.port)
    }

    val socketTimeoutMs = config.controllerSocketTimeoutMs

    def networkClientControlledShutdown(retries: Int): Boolean = {
      val metadataUpdater = new ManualMetadataUpdater()
      val networkClient = {
        val channelBuilder = ChannelBuilders.create(
          config.interBrokerSecurityProtocol,
          Mode.CLIENT,
          LoginType.SERVER,
          config.values,
          config.saslMechanismInterBrokerProtocol,
          config.saslInterBrokerHandshakeRequestEnable)
        val selector = new Selector(
          NetworkReceive.UNLIMITED,
          config.connectionsMaxIdleMs,
          metrics,
          kafkaMetricsTime,
          "kafka-server-controlled-shutdown",
          Map.empty.asJava,
          false,
          channelBuilder
        )
        new NetworkClient(
          selector,
          metadataUpdater,
          config.brokerId.toString,
          1,
          0,
          Selectable.USE_DEFAULT_BUFFER_SIZE,
          Selectable.USE_DEFAULT_BUFFER_SIZE,
          config.requestTimeoutMs,
          kafkaMetricsTime)
      }

      var shutdownSucceeded: Boolean = false

      try {

        var remainingRetries = retries
        var prevController: Broker = null
        var ioException = false

        while (!shutdownSucceeded && remainingRetries > 0) {
          remainingRetries = remainingRetries - 1

          import NetworkClientBlockingOps._

          // 1. Find the controller and establish a connection to it.

          // Get the current controller info. This is to ensure we use the most recent info to issue the
          // controlled shutdown request
          val controllerId = zkUtils.getController()
          zkUtils.getBrokerInfo(controllerId) match {
            case Some(broker) =>
              // if this is the first attempt, if the controller has changed or if an exception was thrown in a previous
              // attempt, connect to the most recent controller
              if (ioException || broker != prevController) {

                ioException = false

                if (prevController != null)
                  networkClient.close(node(prevController).idString)

                prevController = broker
                metadataUpdater.setNodes(Seq(node(prevController)).asJava)
              }
            case None => //ignore and try again
          }

          // 2. issue a controlled shutdown to the controller
          if (prevController != null) {
            try {

              if (!networkClient.blockingReady(node(prevController), socketTimeoutMs))
                throw new SocketTimeoutException(s"Failed to connect within $socketTimeoutMs ms")

              // send the controlled shutdown request
              val requestHeader = networkClient.nextRequestHeader(ApiKeys.CONTROLLED_SHUTDOWN_KEY)
              val send = new RequestSend(node(prevController).idString, requestHeader,
                new ControlledShutdownRequest(config.brokerId).toStruct)
              val request = new ClientRequest(kafkaMetricsTime.milliseconds(), true, send, null)
              val clientResponse = networkClient.blockingSendAndReceive(request)

              val shutdownResponse = new ControlledShutdownResponse(clientResponse.responseBody)
              if (shutdownResponse.errorCode == Errors.NONE.code && shutdownResponse.partitionsRemaining.isEmpty) {
                shutdownSucceeded = true
                info("Controlled shutdown succeeded")
              }
              else {
                info("Remaining partitions to move: %s".format(shutdownResponse.partitionsRemaining.asScala.mkString(",")))
                info("Error code from controller: %d".format(shutdownResponse.errorCode))
              }
            }
            catch {
              case ioe: IOException =>
                ioException = true
                warn("Error during controlled shutdown, possibly because leader movement took longer than the configured socket.timeout.ms: %s".format(ioe.getMessage))
                // ignore and try again
            }
          }
          if (!shutdownSucceeded) {
            Thread.sleep(config.controlledShutdownRetryBackoffMs)
            warn("Retrying controlled shutdown after the previous attempt failed...")
          }
        }
      }
      finally
        networkClient.close()

      shutdownSucceeded
    }

    def blockingChannelControlledShutdown(retries: Int): Boolean = {
      var remainingRetries = retries
      var channel: BlockingChannel = null
      var prevController: Broker = null
      var shutdownSucceeded: Boolean = false
      try {
        while (!shutdownSucceeded && remainingRetries > 0) {
          remainingRetries = remainingRetries - 1

          // 1. Find the controller and establish a connection to it.

          // Get the current controller info. This is to ensure we use the most recent info to issue the
          // controlled shutdown request
          val controllerId = zkUtils.getController()
          zkUtils.getBrokerInfo(controllerId) match {
            case Some(broker) =>
              if (channel == null || prevController == null || !prevController.equals(broker)) {
                // if this is the first attempt or if the controller has changed, create a channel to the most recent
                // controller
                if (channel != null)
                  channel.disconnect()

                channel = new BlockingChannel(broker.getBrokerEndPoint(config.interBrokerSecurityProtocol).host,
                  broker.getBrokerEndPoint(config.interBrokerSecurityProtocol).port,
                  BlockingChannel.UseDefaultBufferSize,
                  BlockingChannel.UseDefaultBufferSize,
                  config.controllerSocketTimeoutMs)
                channel.connect()
                prevController = broker
              }
            case None => //ignore and try again
          }

          // 2. issue a controlled shutdown to the controller
          if (channel != null) {
            var response: NetworkReceive = null
            try {
              // send the controlled shutdown request
              val request = new kafka.api.ControlledShutdownRequest(0, correlationId.getAndIncrement, None, config.brokerId)
              channel.send(request)

              response = channel.receive()
              val shutdownResponse = kafka.api.ControlledShutdownResponse.readFrom(response.payload())
              if (shutdownResponse.errorCode == Errors.NONE.code && shutdownResponse.partitionsRemaining != null &&
                shutdownResponse.partitionsRemaining.size == 0) {
                shutdownSucceeded = true
                info ("Controlled shutdown succeeded")
              }
              else {
                info("Remaining partitions to move: %s".format(shutdownResponse.partitionsRemaining.mkString(",")))
                info("Error code from controller: %d".format(shutdownResponse.errorCode))
              }
            }
            catch {
              case ioe: java.io.IOException =>
                channel.disconnect()
                channel = null
                warn("Error during controlled shutdown, possibly because leader movement took longer than the configured socket.timeout.ms: %s".format(ioe.getMessage))
                // ignore and try again
            }
          }
          if (!shutdownSucceeded) {
            Thread.sleep(config.controlledShutdownRetryBackoffMs)
            warn("Retrying controlled shutdown after the previous attempt failed...")
          }
        }
      }
      finally {
        if (channel != null) {
          channel.disconnect()
          channel = null
        }
      }
      shutdownSucceeded
    }

    if (startupComplete.get() && config.controlledShutdownEnable) {
      // We request the controller to do a controlled shutdown. On failure, we backoff for a configured period
      // of time and try again for a configured number of retries. If all the attempt fails, we simply force
      // the shutdown.
      info("Starting controlled shutdown")

      brokerState.newState(PendingControlledShutdown)

      val shutdownSucceeded =
        // Before 0.9.0.0, `ControlledShutdownRequest` did not contain `client_id` and it's a mandatory field in
        // `RequestHeader`, which is used by `NetworkClient`
        if (config.interBrokerProtocolVersion >= KAFKA_0_9_0)
          networkClientControlledShutdown(config.controlledShutdownMaxRetries.intValue)
        else blockingChannelControlledShutdown(config.controlledShutdownMaxRetries.intValue)

      if (!shutdownSucceeded)
        warn("Proceeding to do an unclean shutdown as all the controlled shutdown attempts failed")

    }
  }

  /**
   * Shutdown API for shutting down a single instance of the Kafka server.
   * Shuts down the LogManager, the SocketServer and the log cleaner scheduler thread
   */
  def shutdown() {
    try {
      info("shutting down")

      if(isStartingUp.get)
        throw new IllegalStateException("Kafka server is still starting up, cannot shut down!")

      val canShutdown = isShuttingDown.compareAndSet(false, true)
      if (canShutdown && shutdownLatch.getCount > 0) {
        CoreUtils.swallow(controlledShutdown())
        brokerState.newState(BrokerShuttingDown)
        if(socketServer != null)
          CoreUtils.swallow(socketServer.shutdown())
        if(requestHandlerPool != null)
          CoreUtils.swallow(requestHandlerPool.shutdown())
        CoreUtils.swallow(kafkaScheduler.shutdown())
        if(apis != null)
          CoreUtils.swallow(apis.close())
        CoreUtils.swallow(authorizer.foreach(_.close()))
        if(replicaManager != null)
          CoreUtils.swallow(replicaManager.shutdown())
        if(logManager != null)
          CoreUtils.swallow(logManager.shutdown())
        if(groupCoordinator != null)
          CoreUtils.swallow(groupCoordinator.shutdown())
        if(kafkaController != null)
          CoreUtils.swallow(kafkaController.shutdown())
        if(zkUtils != null)
          CoreUtils.swallow(zkUtils.close())
        if (metrics != null)
          CoreUtils.swallow(metrics.close())

        brokerState.newState(NotRunning)

        startupComplete.set(false)
        isShuttingDown.set(false)
        AppInfoParser.unregisterAppInfo(jmxPrefix, config.brokerId.toString)
        shutdownLatch.countDown()
        info("shut down completed")
      }
    }
    catch {
      case e: Throwable =>
        fatal("Fatal error during KafkaServer shutdown.", e)
        isShuttingDown.set(false)
        throw e
    }
  }

  /**
   * After calling shutdown(), use this API to wait until the shutdown is complete
   */
  // kafkaServer启动会调用这里，然后阻塞住
  def awaitShutdown(): Unit = shutdownLatch.await()

  def getLogManager(): LogManager = logManager

  def boundPort(protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT): Int = socketServer.boundPort(protocol)

  // 创建logManager
  private def createLogManager(zkClient: ZkClient, brokerState: BrokerState): LogManager = {
    // 加载kafkaConfig中对应LogConfig配置的配置属性值
    val defaultProps = KafkaServer.copyKafkaConfigToLog(config)
    // 生成LogConfig配置
    val defaultLogConfig = LogConfig(defaultProps)
    // 加载zk上的topic相关信息,路径：/brokers/topics
    val configs = AdminUtils.fetchAllTopicConfigs(zkUtils).map { case (topic, configs) =>
      topic -> LogConfig.fromProps(defaultProps, configs)
    }
    // read the log configurations from zookeeper
    // 加载日志清理工具的配置参数
    val cleanerConfig = CleanerConfig(
        // log.cleaner.threads 默认1
        numThreads = config.logCleanerThreads,
        // log.cleaner.dedupe.buffer.size 默认128 * 1024 * 1024L
        dedupeBufferSize = config.logCleanerDedupeBufferSize,
        // log.cleaner.io.buffer.load.factor 默认0.9d
        dedupeBufferLoadFactor = config.logCleanerDedupeBufferLoadFactor,
        // log.cleaner.io.buffer.size 默认 512 * 1024
        ioBufferSize = config.logCleanerIoBufferSize,
        // message.max.bytes 默认 1000000 + 8 + 4
        maxMessageSize = config.messageMaxBytes,
        // log.cleaner.io.max.bytes.per.second 默认 Double.MaxValue
        maxIoBytesPerSecond = config.logCleanerIoMaxBytesPerSecond,
        // log.cleaner.backoff.ms 默认 15 * 1000
        backOffMs = config.logCleanerBackoffMs,
        // log.cleaner.enable 默认 true
        enableCleaner = config.logCleanerEnable)
    // 生成LogManager
    new LogManager(logDirs = config.logDirs.map(new File(_)).toArray,// 日志目录列表，加载的是"log.dirs"为空则加载"log.dir"
                   topicConfigs = configs,//
                   defaultConfig = defaultLogConfig,
                   cleanerConfig = cleanerConfig,
                   ioThreads = config.numRecoveryThreadsPerDataDir,// 默认1
                   flushCheckMs = config.logFlushSchedulerIntervalMs,// Long.MaxValue
                   flushCheckpointMs = config.logFlushOffsetCheckpointIntervalMs,// 60000
                   retentionCheckMs = config.logCleanupIntervalMs,// 5 * 60 * 1000L
                   scheduler = kafkaScheduler,
                   brokerState = brokerState,
                   time = time)
  }

  /**
    * Generates new brokerId if enabled or reads from meta.properties based on following conditions
    * <ol>
    * <li> config has no broker.id provided and broker id generation is enabled, generates a broker.id based on Zookeeper's sequence
    * <li> stored broker.id in meta.properties doesn't match in all the log.dirs throws InconsistentBrokerIdException
    * <li> config has broker.id and meta.properties contains broker.id if they don't match throws InconsistentBrokerIdException
    * <li> config has broker.id and there is no meta.properties file, creates new meta.properties and stores broker.id
    * <ol>
    * @return A brokerId.
    */
  private def getBrokerId: Int =  {
    var brokerId = config.brokerId
    val brokerIdSet = mutable.HashSet[Int]()

    for (logDir <- config.logDirs) {
      val brokerMetadataOpt = brokerMetadataCheckpoints(logDir).read()
      brokerMetadataOpt.foreach { brokerMetadata =>
        brokerIdSet.add(brokerMetadata.brokerId)
      }
    }

    if(brokerIdSet.size > 1)
      throw new InconsistentBrokerIdException(
        s"Failed to match broker.id across log.dirs. This could happen if multiple brokers shared a log directory (log.dirs) " +
        s"or partial data was manually copied from another broker. Found $brokerIdSet")
    else if(brokerId >= 0 && brokerIdSet.size == 1 && brokerIdSet.last != brokerId)
      throw new InconsistentBrokerIdException(
        s"Configured broker.id $brokerId doesn't match stored broker.id ${brokerIdSet.last} in meta.properties. " +
        s"If you moved your data, make sure your configured broker.id matches. " +
        s"If you intend to create a new broker, you should remove all data in your data directories (log.dirs).")
    else if(brokerIdSet.size == 0 && brokerId < 0 && config.brokerIdGenerationEnable)  // generate a new brokerId from Zookeeper
      brokerId = generateBrokerId
    else if(brokerIdSet.size == 1) // pick broker.id from meta.properties
      brokerId = brokerIdSet.last

    brokerId
  }

  private def checkpointBrokerId(brokerId: Int) {
    var logDirsWithoutMetaProps: List[String] = List()

    for (logDir <- config.logDirs) {
      val brokerMetadataOpt = brokerMetadataCheckpoints(logDir).read()
      if(brokerMetadataOpt.isEmpty)
          logDirsWithoutMetaProps ++= List(logDir)
    }

    for(logDir <- logDirsWithoutMetaProps) {
      val checkpoint = brokerMetadataCheckpoints(logDir)
      checkpoint.write(new BrokerMetadata(brokerId))
    }
  }

  private def generateBrokerId: Int = {
    try {
      zkUtils.getBrokerSequenceId(config.maxReservedBrokerId)
    } catch {
      case e: Exception =>
        error("Failed to generate broker.id due to ", e)
        throw new GenerateBrokerIdException("Failed to generate broker.id", e)
    }
  }
}
