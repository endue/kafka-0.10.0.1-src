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

package kafka.network

import java.io.IOException
import java.net._
import java.nio.channels._
import java.nio.channels.{Selector => NSelector}
import java.util
import java.util.concurrent._
import java.util.concurrent.atomic._

import com.yammer.metrics.core.Gauge
import kafka.cluster.{BrokerEndPoint, EndPoint}
import kafka.common.KafkaException
import kafka.metrics.KafkaMetricsGroup
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.network.{ChannelBuilders, KafkaChannel, LoginType, Mode, Selector => KSelector}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.protocol.types.SchemaException
import org.apache.kafka.common.utils.{Time, Utils}

import scala.collection._
import JavaConverters._
import scala.util.control.{ControlThrowable, NonFatal}

/**
 * An NIO socket server. The threading model is
 *   1 Acceptor thread that handles new connections
 *   Acceptor has N Processor threads that each have their own selector and read requests from sockets
 *   M Handler threads that handle requests and produce responses back to the processor threads for writing.
 */
class SocketServer(val config: KafkaConfig, val metrics: Metrics, val time: Time) extends Logging with KafkaMetricsGroup {
  // 配置文件中的listeners列表节点
  private val endpoints = config.listeners
  // 配置文件中的num.network.threads，工作线程数，默认3
  private val numProcessorThreads = config.numNetworkThreads
  // 配置文件中的queued.max.requests，队列中最大请求数，默认500
  private val maxQueuedRequests = config.queuedMaxRequests
  // 总的processor线程数量，一个监听端口默认对应3个
  private val totalProcessorThreads = numProcessorThreads * endpoints.size
  // 配置文件中max.connections.per.ip，每个IP最大连接数，默认Int.MaxValue
  private val maxConnectionsPerIp = config.maxConnectionsPerIp
  // 配置文件中max.connections.per.ip.overrides，指定IP最大连接数
  private val maxConnectionsPerIpOverrides = config.maxConnectionsPerIpOverrides

  this.logIdent = "[Socket Server on Broker " + config.brokerId + "], "
  // 记录每个Processor的请求队列和响应队列
  val requestChannel = new RequestChannel(totalProcessorThreads, maxQueuedRequests)
  // 记录Processor的数组
  private val processors = new Array[Processor](totalProcessorThreads)
  // 记录Acceptor到map中，key是endpoint，value是Acceptor
  private[network] val acceptors = mutable.Map[EndPoint, Acceptor]()
  // 用于限制IP的最大连接数，里面记录了每个IP对应的最大连接数
  private var connectionQuotas: ConnectionQuotas = _

  private val allMetricNames = (0 until totalProcessorThreads).map { i =>
    val tags = new util.HashMap[String, String]()
    tags.put("networkProcessor", i.toString)
    metrics.metricName("io-wait-ratio", "socket-server-metrics", tags)
  }

  /**
   * Start the socket server
   */
  def startup() {
    this.synchronized {
      // connectionQuotas 用于限制IP的最大连接数，里面记录了每个IP对应的最大连接数
      connectionQuotas = new ConnectionQuotas(maxConnectionsPerIp, maxConnectionsPerIpOverrides)
      // 默认100kb
      val sendBufferSize = config.socketSendBufferBytes
      // 默认100kb
      val recvBufferSize = config.socketReceiveBufferBytes
      val brokerId = config.brokerId

      var processorBeginIndex = 0
      // 配置文件中的listeners服务列表
      endpoints.values.foreach { endpoint =>
        // 获取请求类型
        val protocol = endpoint.protocolType
        // numProcessorThreads 默认为3
        val processorEndIndex = processorBeginIndex + numProcessorThreads
        // 创建三个Processor然后记录到processors数组中
        for (i <- processorBeginIndex until processorEndIndex)
          processors(i) = newProcessor(i, connectionQuotas, protocol)
        // 创建一个acceptor，并将上述创建的3个Processor传递进去
        // 内部创建一个nioSelector并启动所有的Processor
        val acceptor = new Acceptor(endpoint, sendBufferSize, recvBufferSize, brokerId,
          processors.slice(processorBeginIndex, processorEndIndex), connectionQuotas)
        // 记录acceptor和对应的endpoint
        acceptors.put(endpoint, acceptor)
        // 启动线程，执行acceptor的run方法
        Utils.newThread("kafka-socket-acceptor-%s-%d".format(protocol.toString, endpoint.port), acceptor, false).start()
        // 阻塞，然后等待Acceptor启动完毕
        acceptor.awaitStartup()

        processorBeginIndex = processorEndIndex
      }
    }

    newGauge("NetworkProcessorAvgIdlePercent",
      new Gauge[Double] {
        def value = allMetricNames.map( metricName =>
          metrics.metrics().get(metricName).value()).sum / totalProcessorThreads
      }
    )

    info("Started " + acceptors.size + " acceptor threads")
  }

  // register the processor threads for notification of responses
  requestChannel.addResponseListener(id => processors(id).wakeup())

  /**
   * Shutdown the socket server
   */
  def shutdown() = {
    info("Shutting down")
    this.synchronized {
      acceptors.values.foreach(_.shutdown)
      processors.foreach(_.shutdown)
    }
    info("Shutdown completed")
  }

  def boundPort(protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT): Int = {
    try {
      acceptors(endpoints(protocol)).serverChannel.socket().getLocalPort
    } catch {
      case e: Exception => throw new KafkaException("Tried to check server's port before server was started or checked for port of non-existing protocol", e)
    }
  }

  /* `protected` for test usage */
  protected[network] def newProcessor(id: Int, connectionQuotas: ConnectionQuotas, protocol: SecurityProtocol): Processor = {
    new Processor(id,
      time,
      config.socketRequestMaxBytes,
      requestChannel,
      connectionQuotas,
      config.connectionsMaxIdleMs,
      protocol,
      config.values,
      metrics
    )
  }

  /* For test usage */
  private[network] def connectionCount(address: InetAddress): Int =
    Option(connectionQuotas).fold(0)(_.get(address))

  /* For test usage */
  private[network] def processor(index: Int): Processor = processors(index)

}

/**
 * A base class with some helper variables and methods
 */
private[kafka] abstract class AbstractServerThread(connectionQuotas: ConnectionQuotas) extends Runnable with Logging {
  // 标记Acceptor是否启动完毕
  private val startupLatch = new CountDownLatch(1)
  // 标记Acceptor是否关闭
  private val shutdownLatch = new CountDownLatch(1)
  // 标记Acceptor是否关闭
  private val alive = new AtomicBoolean(true)

  def wakeup()

  /**
   * Initiates a graceful shutdown by signaling to stop and waiting for the shutdown to complete
   * 关闭Acceptor
   */
  def shutdown(): Unit = {
    alive.set(false)
    wakeup()
    shutdownLatch.await()
  }

  /**
   * Wait for the thread to completely start up
   */
  def awaitStartup(): Unit = startupLatch.await

  /**
   * Record that the thread startup is complete
   */
  protected def startupComplete() = {
    startupLatch.countDown()
  }

  /**
   * Record that the thread shutdown is complete
   */
  protected def shutdownComplete() = shutdownLatch.countDown()

  /**
   * Is the server still running?
    * Accpetor是否还在运行
   */
  protected def isRunning = alive.get

  /**
   * Close the connection identified by `connectionId` and decrement the connection count.
   */
  def close(selector: KSelector, connectionId: String) {
    val channel = selector.channel(connectionId)
    if (channel != null) {
      debug(s"Closing selector connection $connectionId")
      val address = channel.socketAddress
      if (address != null)
        connectionQuotas.dec(address)
      selector.close(connectionId)
    }
  }

  /**
   * Close `channel` and decrement the connection count.
   */
  def close(channel: SocketChannel) {
    if (channel != null) {
      debug("Closing connection from " + channel.socket.getRemoteSocketAddress())
      connectionQuotas.dec(channel.socket.getInetAddress)
      swallowError(channel.socket().close())
      swallowError(channel.close())
    }
  }
}

/**
 * Thread that accepts and configures new connections. There is one of these per endpoint.
  * Acceptor，负责接收和配置新的连接请求，每个endpoint对应一个
 */
private[kafka] class Acceptor(val endPoint: EndPoint,
                              val sendBufferSize: Int,
                              val recvBufferSize: Int,
                              brokerId: Int,
                              processors: Array[Processor],
                              connectionQuotas: ConnectionQuotas) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {
  // 创建一个nioSelector
  private val nioSelector = NSelector.open()
  // 建立socket连接
  val serverChannel = openServerSocket(endPoint.host, endPoint.port)
  // 启动Acceptor内部的Processor
  this.synchronized {
    processors.foreach { processor =>
      Utils.newThread("kafka-network-thread-%d-%s-%d".format(brokerId, endPoint.protocolType.toString, processor.id), processor, false).start()
    }
  }

  /**
   * Accept loop that checks for new connection attempts
    * acceptor不断循环，接受请求
   */
  def run() {
    // 注册监听OP_ACCEPT事件
    serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT)
    // 标记Acceptor启动完毕，并唤醒阻塞等待
    startupComplete()
    try {
      var currentProcessor = 0
      // 不断的while循环，直到Acceptor关闭
      while (isRunning) {
        try {
          // 监听OP_ACCEPT事件,并返回就绪的SelectorKey
          val ready = nioSelector.select(500)
          if (ready > 0) {
            // 遍历所有就绪的SelectorKey
            val keys = nioSelector.selectedKeys()
            val iter = keys.iterator()
            while (iter.hasNext && isRunning) {
              try {
                val key = iter.next
                iter.remove()
                // 如果是OP_ACCEPT连接请求
                if (key.isAcceptable)
                  // 将连接请求交给Processor处理
                  // processors(currentProcessor)是采用轮询的方式将连接请求交给Processor
                  accept(key, processors(currentProcessor))
                else
                  throw new IllegalStateException("Unrecognized key state for acceptor thread.")

                // round robin to the next processor thread
                // 轮询所有的Processor，均匀的处理每个请求
                currentProcessor = (currentProcessor + 1) % processors.length
              } catch {
                case e: Throwable => error("Error while accepting connection", e)
              }
            }
          }
        }
        catch {
          // We catch all the throwables to prevent the acceptor thread from exiting on exceptions due
          // to a select operation on a specific channel or a bad request. We don't want the
          // the broker to stop responding to requests from other clients in these scenarios.
          case e: ControlThrowable => throw e
          case e: Throwable => error("Error occurred", e)
        }
      }
    } finally {
      debug("Closing server socket and selector.")
      swallowError(serverChannel.close())
      swallowError(nioSelector.close())
      shutdownComplete()
    }
  }

  /*
   * Create a server socket to listen for connections on.
   * 创建一个serverSocket并绑定到对应的地址
   */
  private def openServerSocket(host: String, port: Int): ServerSocketChannel = {
    val socketAddress =
      if(host == null || host.trim.isEmpty)
        new InetSocketAddress(port)
      else
        new InetSocketAddress(host, port)
    val serverChannel = ServerSocketChannel.open()
    serverChannel.configureBlocking(false)
    serverChannel.socket().setReceiveBufferSize(recvBufferSize)
    try {
      serverChannel.socket.bind(socketAddress)
      info("Awaiting socket connections on %s:%d.".format(socketAddress.getHostString, serverChannel.socket.getLocalPort))
    } catch {
      case e: SocketException =>
        throw new KafkaException("Socket server failed to bind to %s:%d: %s.".format(socketAddress.getHostString, port, e.getMessage), e)
    }
    serverChannel
  }

  /*
   * Accept a new connection
   * 处理新连接
   */
  def accept(key: SelectionKey, processor: Processor) {
    /**
      * 下面两句话对应java中的写法
      * ServerSocketChannel channel = (ServerSocketChannel) key.channel();
      * SocketChannel client = channel.accept();
      */
    val serverSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]
    val socketChannel = serverSocketChannel.accept()
    try {
      // 记录地址对应的连接请求数
      connectionQuotas.inc(socketChannel.socket().getInetAddress)
      // 与客户端建立socket连接，设置非阻塞等等
      socketChannel.configureBlocking(false)
      socketChannel.socket().setTcpNoDelay(true)
      socketChannel.socket().setKeepAlive(true)
      socketChannel.socket().setSendBufferSize(sendBufferSize)

      debug("Accepted connection from %s on %s and assigned it to processor %d, sendBufferSize [actual|requested]: [%d|%d] recvBufferSize [actual|requested]: [%d|%d]"
            .format(socketChannel.socket.getRemoteSocketAddress, socketChannel.socket.getLocalSocketAddress, processor.id,
                  socketChannel.socket.getSendBufferSize, sendBufferSize,
                  socketChannel.socket.getReceiveBufferSize, recvBufferSize))
      // 连接交给processor处理
      processor.accept(socketChannel)
    } catch {
      case e: TooManyConnectionsException =>
        info("Rejected connection from %s, address already has the configured maximum of %d connections.".format(e.ip, e.count))
        close(socketChannel)
    }
  }

  /**
   * Wakeup the thread for selection.
   */
  @Override
  def wakeup = nioSelector.wakeup()

}

/**
 * Thread that processes all requests from a single connection. There are N of these running in parallel
 * each of which has its own selector
  * Processor,处理所有的请求，多个同时运行并且每个都有自己独立的Selector
 */
private[kafka] class Processor(val id: Int,
                               time: Time,
                               maxRequestSize: Int,
                               requestChannel: RequestChannel,
                               connectionQuotas: ConnectionQuotas,
                               connectionsMaxIdleMs: Long,
                               protocol: SecurityProtocol,
                               channelConfigs: java.util.Map[String, _],
                               metrics: Metrics) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

  private object ConnectionId {
    def fromString(s: String): Option[ConnectionId] = s.split("-") match {
      case Array(local, remote) => BrokerEndPoint.parseHostPort(local).flatMap { case (localHost, localPort) =>
        BrokerEndPoint.parseHostPort(remote).map { case (remoteHost, remotePort) =>
          ConnectionId(localHost, localPort, remoteHost, remotePort)
        }
      }
      case _ => None
    }
  }

  private case class ConnectionId(localHost: String, localPort: Int, remoteHost: String, remotePort: Int) {
    override def toString: String = s"$localHost:$localPort-$remoteHost:$remotePort"
  }
  // 记录新的连接请求
  private val newConnections = new ConcurrentLinkedQueue[SocketChannel]()
  // 记录正在发生或发生中的响应消息
  private val inflightResponses = mutable.Map[String, RequestChannel.Response]()
  private val metricTags = Map("networkProcessor" -> id.toString).asJava

  newGauge("IdlePercent",
    new Gauge[Double] {
      def value = {
        metrics.metrics().get(metrics.metricName("io-wait-ratio", "socket-server-metrics", metricTags)).value()
      }
    },
    metricTags.asScala
  )

  // 初始化Processor对应的Selector
  private val selector = new KSelector(
    maxRequestSize,
    connectionsMaxIdleMs,
    metrics,
    time,
    "socket-server",
    metricTags,
    false,
    ChannelBuilders.create(protocol, Mode.SERVER, LoginType.SERVER, channelConfigs, null, true))

  override def run() {
    startupComplete()
    while (isRunning) {
      try {
        // setup any new connections that have been queued up
        // 配置所有的新连接，并设置新的connnection的socketChannel关注OP_READ事件
        configureNewConnections()
        // register any new responses for writing
        // 处理新的响应，并设置Response对应的socketChannel关注OP_WRITE事件
        processNewResponses()
        // 读或写消息，当新连接建立后，会调用wakeup方法唤醒
        poll()
        // 处理接收完毕的消息，保存到requestChannel的requestQueue中
        // 然后获取当前selector的channel去掉关注的OP_READ事件
        processCompletedReceives()
        // 处理发送完毕的响应，读取selector中completedSends中的消息，然后基于目的地删掉inflightResponses中的记录
        // 并关注对应channel的OP_READ事件
        processCompletedSends()
        // 处理断开的连接
        processDisconnected()
      } catch {
        // We catch all the throwables here to prevent the processor thread from exiting. We do this because
        // letting a processor exit might cause a bigger impact on the broker. Usually the exceptions thrown would
        // be either associated with a specific socket channel or a bad request. We just ignore the bad socket channel
        // or request. This behavior might need to be reviewed if we see an exception that need the entire broker to stop.
        case e: ControlThrowable => throw e
        case e: Throwable =>
          error("Processor got uncaught exception.", e)
      }
    }

    debug("Closing selector - processor " + id)
    swallowError(closeAll())
    shutdownComplete()
  }

  private def processNewResponses() {
    // 基于processor的id获取响应队列responseQueues中对于的BlockingQueue的头节点并返回
    // 返回类型RequestChannel.Response
    var curr = requestChannel.receiveResponse(id)
    while (curr != null) {
      try {
        curr.responseAction match {
          case RequestChannel.NoOpAction =>
            // There is no response to send to the client, we need to read more pipelined requests
            // that are sitting in the server's socket buffer
            curr.request.updateRequestMetrics
            trace("Socket server received empty response to send, registering for read: " + curr)
            // 如果没有响应消息，则关注OP_READ事件
            selector.unmute(curr.request.connectionId)
          case RequestChannel.SendAction =>
            // 发送响应
            // 保存记录到inflightResponses中
            sendResponse(curr)
          case RequestChannel.CloseConnectionAction =>
            curr.request.updateRequestMetrics
            trace("Closing socket connection actively according to the response code.")
            close(selector, curr.request.connectionId)
        }
      } finally {
        curr = requestChannel.receiveResponse(id)
      }
    }
  }

  /* `protected` for test usage */
  protected[network] def sendResponse(response: RequestChannel.Response) {
    trace(s"Socket server received response to send, registering for write and sending data: $response")
    val channel = selector.channel(response.responseSend.destination)
    // `channel` can be null if the selector closed the connection because it was idle for too long
    if (channel == null) {
      warn(s"Attempting to send response via channel for which there is no open connection, connection id $id")
      response.request.updateRequestMetrics()
    }
    else {
      selector.send(response.responseSend)
      inflightResponses += (response.request.connectionId -> response)
    }
  }

  private def poll() {
    try selector.poll(300)
    catch {
      case e @ (_: IllegalStateException | _: IOException) =>
        error(s"Closing processor $id due to illegal state or IO exception")
        swallow(closeAll())
        shutdownComplete()
        throw e
    }
  }

  private def processCompletedReceives() {
    selector.completedReceives.asScala.foreach { receive =>
      try {
        val channel = selector.channel(receive.source)
        val session = RequestChannel.Session(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, channel.principal.getName),
          channel.socketAddress)
        val req = RequestChannel.Request(processor = id, connectionId = receive.source, session = session, buffer = receive.payload, startTimeMs = time.milliseconds, securityProtocol = protocol)
        // 处理接收到的请求并保存到requestQueue中
        requestChannel.sendRequest(req)
        // 去掉当前socketChannel关注的OP_READ事件
        selector.mute(receive.source)
      } catch {
        case e @ (_: InvalidRequestException | _: SchemaException) =>
          // note that even though we got an exception, we can assume that receive.source is valid. Issues with constructing a valid receive object were handled earlier
          error(s"Closing socket for ${receive.source} because of error", e)
          close(selector, receive.source)
      }
    }
  }

  private def processCompletedSends() {
    selector.completedSends.asScala.foreach { send =>
      val resp = inflightResponses.remove(send.destination).getOrElse {
        throw new IllegalStateException(s"Send for ${send.destination} completed, but not in `inflightResponses`")
      }
      resp.request.updateRequestMetrics()
      // 关注OP_READ事件
      selector.unmute(send.destination)
    }
  }

  private def processDisconnected() {
    selector.disconnected.asScala.foreach { connectionId =>
      val remoteHost = ConnectionId.fromString(connectionId).getOrElse {
        throw new IllegalStateException(s"connectionId has unexpected format: $connectionId")
      }.remoteHost
      inflightResponses.remove(connectionId).foreach(_.request.updateRequestMetrics())
      // the channel has been closed by the selector but the quotas still need to be updated
      connectionQuotas.dec(InetAddress.getByName(remoteHost))
    }
  }

  /**
   * Queue up a new connection for reading
    * 记录新建立的连接请求，准备后续的读事件，
    * 由Acceptor接受到OP_ACCEPT事件时写入
   */
  def accept(socketChannel: SocketChannel) {
    newConnections.add(socketChannel)
    // 唤醒selector
    wakeup()
  }

  /**
   * Register any new connections that have been queued up
    * 配置新建立连接的请求
   */
  private def configureNewConnections() {
    while (!newConnections.isEmpty) {
      // 获取对应的channel，阻塞式
      val channel = newConnections.poll()
      try {
        debug(s"Processor $id listening to new connection from ${channel.socket.getRemoteSocketAddress}")
        // 获取本地服务的IP和prot
        val localHost = channel.socket().getLocalAddress.getHostAddress
        val localPort = channel.socket().getLocalPort
        // 获取远程服务的IP和prot
        val remoteHost = channel.socket().getInetAddress.getHostAddress
        val remotePort = channel.socket().getPort
        // 初始化一个connectionId
        val connectionId = ConnectionId(localHost, localPort, remoteHost, remotePort).toString
        // 将connectionId和对应的channel注册到selector
        selector.register(connectionId, channel)
      } catch {
        // We explicitly catch all non fatal exceptions and close the socket to avoid a socket leak. The other
        // throwables will be caught in processor and logged as uncaught exceptions.
        case NonFatal(e) =>
          // need to close the channel here to avoid a socket leak.
          close(channel)
          error(s"Processor $id closed connection from ${channel.getRemoteAddress}", e)
      }
    }
  }

  /**
   * Close the selector and all open connections
   */
  private def closeAll() {
    selector.channels.asScala.foreach { channel =>
      close(selector, channel.id)
    }
    selector.close()
  }

  /* For test usage */
  private[network] def channel(connectionId: String): Option[KafkaChannel] =
    Option(selector.channel(connectionId))

  /**
   * Wakeup the thread for selection.
   */
  @Override
  def wakeup = selector.wakeup()

}

/**
  * 记录地址的连接数，默认最大运行值为
  * @param defaultMax
  * @param overrideQuotas
  */
class ConnectionQuotas(val defaultMax: Int, overrideQuotas: Map[String, Int]) {

  private val overrides = overrideQuotas.map { case (host, count) => (InetAddress.getByName(host), count) }
  // 记录地址对应的连接数
  private val counts = mutable.Map[InetAddress, Int]()
  // 增加地址对应的连接数
  def inc(address: InetAddress) {
    counts.synchronized {
      val count = counts.getOrElseUpdate(address, 0)
      counts.put(address, count + 1)
      // 获取地址对应的运行的最大连接数，如果
      val max = overrides.getOrElse(address, defaultMax)
      if (count >= max)
        throw new TooManyConnectionsException(address, max)
    }
  }
  // 递减地址对应的连接数
  def dec(address: InetAddress) {
    counts.synchronized {
      val count = counts.getOrElse(address,
        throw new IllegalArgumentException(s"Attempted to decrease connection count for address with no connections, address: $address"))
      if (count == 1)
        counts.remove(address)
      else
        counts.put(address, count - 1)
    }
  }
  // 获取地址对应的连接数
  def get(address: InetAddress): Int = counts.synchronized {
    counts.getOrElse(address, 0)
  }

}

class TooManyConnectionsException(val ip: InetAddress, val count: Int) extends KafkaException("Too many connections from %s (maximum = %d)".format(ip, count))
