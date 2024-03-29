/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestSend;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * A network client for asynchronous request/response network i/o. This is an internal class used to implement the
 * user-facing producer and consumer clients.
 * <p>
 * This class is not thread-safe!
 */
public class NetworkClient implements KafkaClient {

    private static final Logger log = LoggerFactory.getLogger(NetworkClient.class);

    /* the selector used to perform network i/o */
    private final Selectable selector;

    private final MetadataUpdater metadataUpdater;

    private final Random randOffset;

    /* the state of each node's connection */
    // 记录每个节点的连接状态
    private final ClusterConnectionStates connectionStates;

    /* the set of requests currently being sent or awaiting a response */
    // 获取"max.in.flight.requests.per.connection",默认5
    // 客户端在单个服务连接上发送的未确认消息的最大数量
    private final InFlightRequests inFlightRequests;

    /* the socket send buffer size in bytes */
    // 获取 "send.buffer.bytes",默认128 * 1024
    // 发送数据时的tcp缓冲区
    private final int socketSendBuffer;

    /* the socket receive size buffer in bytes */
    // 获取"receive.buffer.bytes",默认32 * 1024
    // 接收数据时的tcp缓冲区
    private final int socketReceiveBuffer;

    /* the client id used to identify this client in requests to the server */
    // 客户端id，用于在向服务器发出的请求中标识此客户端
    private final String clientId;

    /* the current correlation id to use when sending requests to servers */
    // 当向服务端发送请求时使用
    private int correlation;

    /* max time in ms for the producer to wait for acknowledgement from server*/
    // 请求超时时间
    private final int requestTimeoutMs;

    private final Time time;

    public NetworkClient(Selectable selector,
                         Metadata metadata,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         int socketSendBuffer,
                         int socketReceiveBuffer,
                         int requestTimeoutMs,
                         Time time) {
        this(null, metadata, selector, clientId, maxInFlightRequestsPerConnection,
                reconnectBackoffMs, socketSendBuffer, socketReceiveBuffer, requestTimeoutMs, time);
    }

    public NetworkClient(Selectable selector,
                         MetadataUpdater metadataUpdater,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         int socketSendBuffer,
                         int socketReceiveBuffer,
                         int requestTimeoutMs,
                         Time time) {
        this(metadataUpdater, null, selector, clientId, maxInFlightRequestsPerConnection, reconnectBackoffMs,
                socketSendBuffer, socketReceiveBuffer, requestTimeoutMs, time);
    }

    private NetworkClient(MetadataUpdater metadataUpdater,
                          Metadata metadata,
                          Selectable selector,
                          String clientId,
                          int maxInFlightRequestsPerConnection,
                          long reconnectBackoffMs,
                          int socketSendBuffer,
                          int socketReceiveBuffer,
                          int requestTimeoutMs,
                          Time time) {

        /* It would be better if we could pass `DefaultMetadataUpdater` from the public constructor, but it's not
         * possible because `DefaultMetadataUpdater` is an inner class and it can only be instantiated after the
         * super constructor is invoked.
         */
        if (metadataUpdater == null) {
            if (metadata == null)
                throw new IllegalArgumentException("`metadata` must not be null");
            this.metadataUpdater = new DefaultMetadataUpdater(metadata);
        } else {
            this.metadataUpdater = metadataUpdater;
        }
        this.selector = selector;
        this.clientId = clientId;
        this.inFlightRequests = new InFlightRequests(maxInFlightRequestsPerConnection);
        this.connectionStates = new ClusterConnectionStates(reconnectBackoffMs);
        this.socketSendBuffer = socketSendBuffer;
        this.socketReceiveBuffer = socketReceiveBuffer;
        this.correlation = 0;
        this.randOffset = new Random();
        this.requestTimeoutMs = requestTimeoutMs;
        this.time = time;
    }

    /**
     * Begin connecting to the given node, return true if we are already connected and ready to send to that node.
     * 开始连接到给定节点，如果已经连接并准备发送到该节点，则返回true
     * @param node The node to check
     * @param now The current timestamp
     * @return True if we are ready to send to the given node
     */
    @Override
    public boolean ready(Node node, long now) {
        if (node.isEmpty())
            throw new IllegalArgumentException("Cannot connect to empty node " + node);
        // 当前node节点连接是否可用
        if (isReady(node, now))
            return true;
        // 判断是否可以建立连接:
        //  没有连接
        //  未创建连接 || (已断开 && 符合重连时间(默认50ms))
        if (connectionStates.canConnect(node.idString(), now))
            // if we are interested in sending to a node and we don't have a connection to it, initiate one
            // 重新建立连接
            initiateConnect(node, now);

        return false;
    }

    /**
     * Closes the connection to a particular node (if there is one).
     *
     * @param nodeId The id of the node
     */
    @Override
    public void close(String nodeId) {
        selector.close(nodeId);
        for (ClientRequest request : inFlightRequests.clearAll(nodeId))
            metadataUpdater.maybeHandleDisconnection(request);
        connectionStates.remove(nodeId);
    }

    /**
     * Returns the number of milliseconds to wait, based on the connection state, before attempting to send data. When
     * disconnected, this respects the reconnect backoff time. When connecting or connected, this handles slow/stalled
     * connections.
     *
     * @param node The node to check
     * @param now The current timestamp
     * @return The number of milliseconds to wait.
     */
    @Override
    public long connectionDelay(Node node, long now) {
        return connectionStates.connectionDelay(node.idString(), now);
    }

    /**
     * Check if the connection of the node has failed, based on the connection state. Such connection failure are
     * usually transient and can be resumed in the next {@link #ready(org.apache.kafka.common.Node, long)} }
     * call, but there are cases where transient failures needs to be caught and re-acted upon.
     *
     * @param node the node to check
     * @return true iff the connection has failed and the node is disconnected
     */
    @Override
    public boolean connectionFailed(Node node) {
        return connectionStates.connectionState(node.idString()).equals(ConnectionState.DISCONNECTED);
    }

    /**
     * Check if the node with the given id is ready to send more requests.
     *
     * @param node The node
     * @param now The current time in ms
     * @return true if the node is ready
     */
    @Override
    public boolean isReady(Node node, long now) {
        // if we need to update our metadata now declare all requests unready to make metadata requests first
        // priority
        return !metadataUpdater.isUpdateDue(now) && canSendRequest(node.idString());
    }

    /**
     * Are we connected and ready and able to send more requests to the given connection?
     *
     * @param node The node
     */
    private boolean canSendRequest(String node) {
        return connectionStates.isConnected(node) && selector.isChannelReady(node) && inFlightRequests.canSendMore(node);
    }

    /**
     * Queue up the given request for sending. Requests can only be sent out to ready nodes.
     * 发送消息，将消息赋值给对应的KafkaChannel上并关注OP_WRITE事件
     *
     * @param request The request
     * @param now The current timestamp
     */
    @Override
    public void send(ClientRequest request, long now) {
        // 获取请求发送的目的地
        String nodeId = request.request().destination();
        // 判断是否允许发送
        if (!canSendRequest(nodeId))
            throw new IllegalStateException("Attempt to send a request to node " + nodeId + " which is not ready.");
        doSend(request, now);
    }

    private void doSend(ClientRequest request, long now) {
        request.setSendTimeMs(now);
        // 这里又将request暂存到inFlightRequests的Map<String, Deque<ClientRequest>> requests中,调用的是addFirst()
        this.inFlightRequests.add(request);
        // 将发送的消息request保存到对应KafkaChannel的Send属性中并关注OP_WRITE事件
        selector.send(request.request());
    }

    /**
     * Do actual reads and writes to sockets.
     *
     * @param timeout The maximum amount of time to wait (in ms) for responses if there are none immediately,
     *                must be non-negative. The actual timeout will be the minimum of timeout, request timeout and
     *                metadata timeout
     * @param now The current time in milliseconds
     * @return The list of responses received
     */
    @Override
    public List<ClientResponse> poll(long timeout, long now) {
        // 获取metadata当前时间距离上次刷新时的时间戳，如果可以将更新metadata
        long metadataTimeout = metadataUpdater.maybeUpdate(now);
        try {
            // 处理OP_READ、OP_WRITE、OP_CONNECT事件
            // 也会处理空闲的连接,将其关闭
            this.selector.poll(Utils.min(timeout, metadataTimeout, requestTimeoutMs));
        } catch (IOException e) {
            log.error("Unexpected error during I/O", e);
        }

        // process completed actions
        long updatedNow = this.time.milliseconds();
        List<ClientResponse> responses = new ArrayList<>();
        // 处理不需要回调的发送请求，封装回调到responses中
        //  创建的ClientResponse为ClientResponse(request, now, false, null)
        handleCompletedSends(responses, updatedNow);
        // 处理接受到的响应，
        // 如果是metadata更新请求的响应信息则不记录到上述responses中,如果是其他请求的响应
        //  创建的ClientResponse为ClientResponse(req, now, false, body)
        handleCompletedReceives(responses, updatedNow);
        // 处理断开的链接
        // 修改节点对应的状态为DISCONNECTED
        //  创建的ClientResponse为ClientResponse(request, now, true, null)
        handleDisconnections(responses, updatedNow);
        // 处理建立连接的节点，修改状态为CONNECTED
        handleConnections();
        // 处理inFlightRequests中已发送请求还未收到响应的超时请求
        //  创建的ClientResponse为ClientResponse(request, now, true, null)
        handleTimedOutRequests(responses, updatedNow);

        // invoke callbacks
        // 执行callback回调
        for (ClientResponse response : responses) {
            if (response.request().hasCallback()) {
                try {
                    // 跳转到Sender.produceRequest里面创建的回调方法
                    response.request().callback().onComplete(response);
                } catch (Exception e) {
                    log.error("Uncaught error in request completion:", e);
                }
            }
        }

        return responses;
    }

    /**
     * Get the number of in-flight requests
     */
    @Override
    public int inFlightRequestCount() {
        return this.inFlightRequests.inFlightRequestCount();
    }

    /**
     * Get the number of in-flight requests for a given node
     */
    @Override
    public int inFlightRequestCount(String node) {
        return this.inFlightRequests.inFlightRequestCount(node);
    }

    /**
     * Generate a request header for the given API key
     *
     * @param key The api key
     * @return A request header with the appropriate client id and correlation id
     */
    @Override
    public RequestHeader nextRequestHeader(ApiKeys key) {
        return new RequestHeader(key.id, clientId, correlation++);
    }

    /**
     * Generate a request header for the given API key and version
     *
     * @param key The api key
     * @param version The api version
     * @return A request header with the appropriate client id and correlation id
     */
    @Override
    public RequestHeader nextRequestHeader(ApiKeys key, short version) {
        return new RequestHeader(key.id, version, clientId, correlation++);
    }

    /**
     * Interrupt the client if it is blocked waiting on I/O.
     */
    @Override
    public void wakeup() {
        // 当B线程阻塞在select()或select(long)方法上时，A线程调用wakeup后，B线程会立刻返回。
        // 如果没有线程阻塞在select()方法上，那么下一次某个线程调用select()或select(long)方法时会立刻返回。
        // 如果调用selector.wakeup()方法，当前没有阻塞在select()上，那么会影响下一次调用select()。
        this.selector.wakeup();
    }

    /**
     * Close the network client
     */
    @Override
    public void close() {
        this.selector.close();
    }

    /**
     * Choose the node with the fewest outstanding requests which is at least eligible for connection. This method will
     * prefer a node with an existing connection, but will potentially choose a node for which we don't yet have a
     * connection if all existing connections are in use. This method will never choose a node for which there is no
     * existing connection and from which we have disconnected within the reconnect backoff period.
     * 选择一个未完成请求最少并且符合连接条件的节点。此方法将首选具有现有连接的节点，但如果所有现有连接都在使用中，
     * 则可能会选择尚未连接的节点。此方法永远不会选择不存在现有连接的节点，也不会选择在重新连接回退期间已断开连接的节点
     * @return The node with the fewest in-flight requests.
     */
    @Override
    public Node leastLoadedNode(long now) {
        // 获取所有的kafak broker
        List<Node> nodes = this.metadataUpdater.fetchNodes();
        int inflight = Integer.MAX_VALUE;
        Node found = null;
        // 获取随机下标
        int offset = this.randOffset.nextInt(nodes.size());
        for (int i = 0; i < nodes.size(); i++) {
            // 计算一个node下标并获取对应node
            int idx = (offset + i) % nodes.size();
            Node node = nodes.get(idx);
            // 计算对node节点已发送或正在发送没有收到响应的消息集合大小
            int currInflight = this.inFlightRequests.inFlightRequestCount(node.idString());
            // 如果为0 && 当前节点可连接，那么返回该节点
            if (currInflight == 0 && this.connectionStates.isConnected(node.idString())) {
                // if we find an established connection with no in-flight requests we can stop right away
                return node;
            } else if (!this.connectionStates.isBlackedOut(node.idString(), now) && currInflight < inflight) {
                // otherwise if this is the best we have found so far, record that
                inflight = currInflight;
                found = node;
            }
        }

        return found;
    }

    public static Struct parseResponse(ByteBuffer responseBuffer, RequestHeader requestHeader) {
        ResponseHeader responseHeader = ResponseHeader.parse(responseBuffer);
        // Always expect the response version id to be the same as the request version id
        short apiKey = requestHeader.apiKey();
        short apiVer = requestHeader.apiVersion();
        Struct responseBody = ProtoUtils.responseSchema(apiKey, apiVer).read(responseBuffer);
        correlate(requestHeader, responseHeader);
        return responseBody;
    }

    /**
     * Post process disconnection of a node
     *
     * @param responses The list of responses to update
     * @param nodeId Id of the node to be disconnected
     * @param now The current time
     */
    // 处理断开连接的节点
    private void processDisconnection(List<ClientResponse> responses, String nodeId, long now) {
        // 1.标记状态为DISCONNECTED
        connectionStates.disconnected(nodeId, now);
        // 2.将发送到该节点的请求丢弃,
        for (ClientRequest request : this.inFlightRequests.clearAll(nodeId)) {
            log.trace("Cancelled request {} due to node {} being disconnected", request, nodeId);
            // 2.1 如果是metadata更新请求,那么将metadataFetchInProgress设置为false并返回true,否则返回false
            if (!metadataUpdater.maybeHandleDisconnection(request))
                // 2.1.1 封装响应,将断开连接的标识设置为true
                responses.add(new ClientResponse(request, now, true, null));
        }
    }

    /**
     * Iterate over all the inflight requests and expire any requests that have exceeded the configured requestTimeout.
     * The connection to the node associated with the request will be terminated and will be treated as a disconnection.
     *
     * @param responses The list of responses to update
     * @param now The current time
     */
    private void handleTimedOutRequests(List<ClientResponse> responses, long now) {
        // 获取超时请求的nodeId
        List<String> nodeIds = this.inFlightRequests.getNodesWithTimedOutRequests(now, this.requestTimeoutMs);
        for (String nodeId : nodeIds) {
            // close connection to the node
            // 关闭与该node节点的连接
            this.selector.close(nodeId);
            log.debug("Disconnecting from node {} due to request timeout.", nodeId);
            // 处理断开连接的节点
            processDisconnection(responses, nodeId, now);
        }

        // we disconnected, so we should probably refresh our metadata
        // 标记需要更新metadata元数据
        if (nodeIds.size() > 0)
            metadataUpdater.requestUpdate();
    }

    /**
     * Handle any completed request send. In particular if no response is expected consider the request complete.
     * 处理任何已完成的请求发送。特别是在不需要响应的情况下，认为请求已完成
     *
     * @param responses The list of responses to update
     * @param now The current time
     */
    private void handleCompletedSends(List<ClientResponse> responses, long now) {
        // if no response is expected then when the send is completed, return it
        for (Send send : this.selector.completedSends()) {
            // 这里只是将发送到对应broker上的请求拿出来，此时还有没出队，底层是peekFirst()
            ClientRequest request = this.inFlightRequests.lastSent(send.destination());
            // 判断是否需要等待服务器响应，如果不需要，那么将inFlightRequests中保存的对应的消息出队
            if (!request.expectResponse()) {
                this.inFlightRequests.completeLastSent(send.destination());
                // 封装一个响应消息
                responses.add(new ClientResponse(request, now, false, null));
            }
        }
    }

    /**
     * Handle any completed receives and update the response list with the responses received.
     * 处理接收到的服务器响应消息，然后在inFlightRequests找到对应的客户端请求并出队
     * 如果是元数据更新的响应消息，则maybeHandleCompletedReceive会处理并返回true
     * 如果不是，封装一个响应到responses中
     * @param responses The list of responses to update
     * @param now The current time
     */
    private void handleCompletedReceives(List<ClientResponse> responses, long now) {
        for (NetworkReceive receive : this.selector.completedReceives()) {
            String source = receive.source();
            // 获取当前客户端发送到响应数据节点的待ack的最老的一个请求
            ClientRequest req = inFlightRequests.completeNext(source);
            // 内部解析响应并校验响应消息和请求消息的关联ID(correlationId)是否一致
            Struct body = parseResponse(receive.payload(), req.request().header());
            // 判断如果是更新元数据请求对应的响应，则不不进入if，否则进入if封装回调
            if (!metadataUpdater.maybeHandleCompletedReceive(req, now, body))
                responses.add(new ClientResponse(req, now, false, body));
        }
    }

    /**
     * Handle any disconnected connections
     * 处理断开的连接
     * @param responses The list of responses that completed with the disconnection
     * @param now The current time
     */
    private void handleDisconnections(List<ClientResponse> responses, long now) {
        // 遍历断开连接的截断
        for (String node : this.selector.disconnected()) {
            log.debug("Node {} disconnected.", node);
            processDisconnection(responses, node, now);
        }
        // we got a disconnect so we should probably refresh our metadata and see if that broker is dead
        // 如果有断开的连接,则标识metadata需要更新
        if (this.selector.disconnected().size() > 0)
            metadataUpdater.requestUpdate();
    }

    /**
     * Record any newly completed connections
     */
    // 处理新建立的连接,记录节点对应的状态为CONNECTED
    private void handleConnections() {
        for (String node : this.selector.connected()) {
            log.debug("Completed connection to node {}", node);
            this.connectionStates.connected(node);
        }
    }

    /**
     * Validate that the response corresponds to the request we expect or else explode
     */
    // 验证响应是否与我们期望的请求相对应
    private static void correlate(RequestHeader requestHeader, ResponseHeader responseHeader) {
        if (requestHeader.correlationId() != responseHeader.correlationId())
            throw new IllegalStateException("Correlation id for response (" + responseHeader.correlationId()
                    + ") does not match request (" + requestHeader.correlationId() + ")");
    }

    /**
     * Initiate a connection to the given node
     */
    // 初始化与给定节点的连接
    private void initiateConnect(Node node, long now) {
        String nodeConnectionId = node.idString();
        try {
            log.debug("Initiating connection to node {} at {}:{}.", node.id(), node.host(), node.port());
            // 为当前节点封装一个connectionStates，状态为CONNECTING
            this.connectionStates.connecting(nodeConnectionId, now);
            // 建立连接，底层基于SocketChannel实现
            selector.connect(nodeConnectionId,
                             new InetSocketAddress(node.host(), node.port()),
                             this.socketSendBuffer,
                             this.socketReceiveBuffer);
        } catch (IOException e) {
            /* attempt failed, we'll try again after the backoff */
            connectionStates.disconnected(nodeConnectionId, now);
            /* maybe the problem is our metadata, update it */
            metadataUpdater.requestUpdate();
            log.debug("Error connecting to node {} at {}:{}:", node.id(), node.host(), node.port(), e);
        }
    }

    class DefaultMetadataUpdater implements MetadataUpdater {

        /* the current cluster metadata */
        // 记录当前集群元数据
        private final Metadata metadata;

        /* true iff there is a metadata request that has been sent and for which we have not yet received a response */
        // 记录当前是否有一个元数据请求已经发送并且还没有收到回应
        private boolean metadataFetchInProgress;

        /* the last timestamp when no broker node is available to connect */
        // 记录没有可用node节点时的时间戳
        private long lastNoNodeAvailableMs;

        DefaultMetadataUpdater(Metadata metadata) {
            this.metadata = metadata;
            this.metadataFetchInProgress = false;
            this.lastNoNodeAvailableMs = 0;
        }

        @Override
        public List<Node> fetchNodes() {
            return metadata.fetch().nodes();
        }

        /**
         * 是否需要触发更新操作，以下任一返回true
         *  1.当前没有发生更新metadata的请求
         *  2.下一个更新操作的时间为0
         * @param now
         * @return
         */
        @Override
        public boolean isUpdateDue(long now) {
            return !this.metadataFetchInProgress && this.metadata.timeToNextUpdate(now) == 0;
        }

        /**
         * 计算metadata上次刷新距离当前时间的时间戳
         * @param now
         * @return
         */
        @Override
        public long maybeUpdate(long now) {
            // should we update our metadata?
            // metadata是否应该更新
            // 获取下次更新metadata的时间戳，如果needUpdate=true，返回0
            long timeToNextMetadataUpdate = metadata.timeToNextUpdate(now);
            // 如果连接失败后等待重试的时间
            long timeToNextReconnectAttempt = Math.max(this.lastNoNodeAvailableMs + metadata.refreshBackoff() - now, 0);
            // 如果一条metadata的fetch请求还未从server收到回复,那么时间设置为Integer.MAX_VALUE
            long waitForMetadataFetch = this.metadataFetchInProgress ? Integer.MAX_VALUE : 0;
            // if there is no node available to connect, back off refreshing metadata
            long metadataTimeout = Math.max(Math.max(timeToNextMetadataUpdate, timeToNextReconnectAttempt),
                    waitForMetadataFetch);
            // 已经到达更新的时间
            if (metadataTimeout == 0) {
                // Beware that the behavior of this method and the computation of timeouts for poll() are
                // highly dependent on the behavior of leastLoadedNode.
                // 选择一个连接数最小的Node
                Node node = leastLoadedNode(now);
                maybeUpdate(now, node);
            }

            return metadataTimeout;
        }

        /**
         * 处理发往到已断开连接节点的请求,如果请求是Metadata请求,那么表示metadataFetchInProgress为false,
         * 方便后续metadata更新请求的发送
         * @param request
         * @return
         */
        @Override
        public boolean maybeHandleDisconnection(ClientRequest request) {
            ApiKeys requestKey = ApiKeys.forId(request.request().header().apiKey());

            if (requestKey == ApiKeys.METADATA) {
                Cluster cluster = metadata.fetch();
                if (cluster.isBootstrapConfigured()) {
                    int nodeId = Integer.parseInt(request.request().destination());
                    Node node = cluster.nodeById(nodeId);
                    if (node != null)
                        log.warn("Bootstrap broker {}:{} disconnected", node.host(), node.port());
                }

                metadataFetchInProgress = false;
                return true;
            }

            return false;
        }

        /**
         * 处理borker返回的元数据请求对应的响应
         * @param req 请求
         * @param now 当前时间戳
         * @param body 响应
         * @return
         */
        @Override
        public boolean maybeHandleCompletedReceive(ClientRequest req, long now, Struct body) {
            short apiKey = req.request().header().apiKey();
            if (apiKey == ApiKeys.METADATA.id && req.isInitiatedByNetworkClient()) {
                handleResponse(req.request().header(), body, now);
                return true;
            }
            return false;
        }

        @Override
        public void requestUpdate() {
            this.metadata.requestUpdate();
        }

        /**
         * 处理更新Metadata请求的响应
         * @param header
         * @param body
         * @param now
         */
        private void handleResponse(RequestHeader header, Struct body, long now) {
            // 将metadataFetchInProgress置为false
            this.metadataFetchInProgress = false;
            // 转换响应
            MetadataResponse response = new MetadataResponse(body);
            // 根据响应生成Cluster
            Cluster cluster = response.cluster();
            // check if any topics metadata failed to get updated
            Map<String, Errors> errors = response.errors();
            if (!errors.isEmpty())
                log.warn("Error while fetching metadata with correlation id {} : {}", header.correlationId(), errors);

            // don't update the cluster if there are no valid nodes...the topic we want may still be in the process of being
            // created which means we will get errors and no nodes until it exists
            // 如果有有效节点信息,那么更新metadata
            if (cluster.nodes().size() > 0) {
                this.metadata.update(cluster, now);
            } else {
                log.trace("Ignoring empty metadata response with correlation id {}.", header.correlationId());
                this.metadata.failedUpdate(now);
            }
        }

        /**
         * Create a metadata request for the given topics
         */
        // 根据给的的元数据请求创建一个客户端请求
        private ClientRequest request(long now, String node, MetadataRequest metadata) {
            RequestSend send = new RequestSend(node, nextRequestHeader(ApiKeys.METADATA), metadata.toStruct());
            return new ClientRequest(now, true, send, null, true);
        }

        /**
         * Add a metadata request to the list of sends if we can make one
         * 如果可以的话，在发送列表中添加一个更新元数据的请求
         */
        private void maybeUpdate(long now, Node node) {
            if (node == null) {
                log.debug("Give up sending metadata request since no node is available");
                // mark the timestamp for no node available to connect
                this.lastNoNodeAvailableMs = now;
                return;
            }
            String nodeConnectionId = node.idString();
            // 当前node是否可以发送请求
            if (canSendRequest(nodeConnectionId)) {
                // 准备开始发送数据,将metadataFetchInProgress置为true
                this.metadataFetchInProgress = true;
                // 创建metadata请求
                MetadataRequest metadataRequest;
                // 创建需要拉取的topic
                if (metadata.needMetadataForAllTopics())
                    metadataRequest = MetadataRequest.allTopics();
                else
                    // 基于metadata中的topics集合进行拉取
                    metadataRequest = new MetadataRequest(new ArrayList<>(metadata.topics()));
                // 封装请求
                ClientRequest clientRequest = request(now, nodeConnectionId, metadataRequest);
                log.debug("Sending metadata request {} to node {}", metadataRequest, node.id());
                // 发送
                doSend(clientRequest, now);
            // 如果可以重新建立连接
            } else if (connectionStates.canConnect(nodeConnectionId, now)) {
                // we don't have a connection to this node right now, make one
                log.debug("Initialize connection to node {} for sending metadata request", node.id());
                // 建立连接
                initiateConnect(node, now);
                // If initiateConnect failed immediately, this node will be put into blackout and we
                // should allow immediately retrying in case there is another candidate node. If it
                // is still connecting, the worst case is that we end up setting a longer timeout
                // on the next round and then wait for the response.
            } else { // connected, but can't send more OR connecting
                // In either case, we just need to wait for a network event to let us know the selected
                // connection might be usable again.
                this.lastNoNodeAvailableMs = now;
            }
        }

    }

}
