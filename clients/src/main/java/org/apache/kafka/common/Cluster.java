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
package org.apache.kafka.common;

import org.apache.kafka.common.utils.Utils;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A representation of a subset of the nodes, topics, and partitions in the Kafka cluster.
 * 该类记录Kafka集群中节点、主题和分区相关信息
 */
public final class Cluster {

    private final boolean isBootstrapConfigured;
    // 记录kafka集群节点，也就是kafka broker
    private final List<Node> nodes;
    // 记录为授权的topic
    private final Set<String> unauthorizedTopics;
    // 记录【主题与其所有分区】关系
    private final Map<TopicPartition, PartitionInfo> partitionsByTopicPartition;
    // 记录【主题与其所有分区】关系
    private final Map<String, List<PartitionInfo>> partitionsByTopic;
    // 记录【主题与其可用分区】关系
    private final Map<String, List<PartitionInfo>> availablePartitionsByTopic;
    // 记录【node节点与其可用分区】关系
    private final Map<Integer, List<PartitionInfo>> partitionsByNode;
    // 记录【node节点与其id】关系
    private final Map<Integer, Node> nodesById;

    /**
     * Create a new cluster with the given nodes and partitions
     * @param nodes The nodes in the cluster
     * @param partitions Information about a subset of the topic-partitions this cluster hosts
     */
    public Cluster(Collection<Node> nodes,
                   Collection<PartitionInfo> partitions,
                   Set<String> unauthorizedTopics) {
        this(false, nodes, partitions, unauthorizedTopics);
    }

    /**
     * 根据参数初始化所有的属性
     * @param isBootstrapConfigured
     * @param nodes
     * @param partitions
     * @param unauthorizedTopics
     */
    private Cluster(boolean isBootstrapConfigured,
                    Collection<Node> nodes,
                    Collection<PartitionInfo> partitions,
                    Set<String> unauthorizedTopics) {
        this.isBootstrapConfigured = isBootstrapConfigured;

        // make a randomized, unmodifiable copy of the nodes
        // 生成的nodes节点，只允许get操作，任何修改操作都将报错
        List<Node> copy = new ArrayList<>(nodes);
        Collections.shuffle(copy);
        this.nodes = Collections.unmodifiableList(copy);
        // 记录node节点与其ID的关系到nodesById
        this.nodesById = new HashMap<>();
        for (Node node : nodes)
            this.nodesById.put(node.id(), node);

        // index the partitions by topic/partition for quick lookup
        // 将参数partitions按照主题、分区的关系记录到partitionsByTopicPartition
        this.partitionsByTopicPartition = new HashMap<>(partitions.size());
        for (PartitionInfo p : partitions)
            // todo 这里key是new TopicPartition，当一个主题存在多个分区是，partitionsByTopicPartition里会存在多条记录
            this.partitionsByTopicPartition.put(new TopicPartition(p.topic(), p.partition()), p);

        // 分别按主题和节点索引分区
        // index the partitions by topic and node respectively, and make the lists
        // unmodifiable so we can hand them out in user-facing apis without risk
        // of the client modifying the contents
        HashMap<String, List<PartitionInfo>> partsForTopic = new HashMap<>();
        HashMap<Integer, List<PartitionInfo>> partsForNode = new HashMap<>();
        // 将nodes也就是kafka broker按照ID进行分类
        for (Node n : this.nodes) {
            partsForNode.put(n.id(), new ArrayList<PartitionInfo>());
        }
        // 将分区信息按照主题分类
        for (PartitionInfo p : partitions) {
            if (!partsForTopic.containsKey(p.topic()))
                partsForTopic.put(p.topic(), new ArrayList<PartitionInfo>());
            List<PartitionInfo> psTopic = partsForTopic.get(p.topic());
            psTopic.add(p);

            if (p.leader() != null) {
                List<PartitionInfo> psNode = Utils.notNull(partsForNode.get(p.leader().id()));
                psNode.add(p);
            }
        }
        this.partitionsByTopic = new HashMap<>(partsForTopic.size());
        this.availablePartitionsByTopic = new HashMap<>(partsForTopic.size());
        for (Map.Entry<String, List<PartitionInfo>> entry : partsForTopic.entrySet()) {
            // 获取主题和对应的分区列表
            String topic = entry.getKey();
            List<PartitionInfo> partitionList = entry.getValue();
            // 将主题与其所有分区信息记录到partitionsByTopic
            this.partitionsByTopic.put(topic, Collections.unmodifiableList(partitionList));
            // 遍历主题所有分区信息，如果leader不为null，就代表是一个可用node
            List<PartitionInfo> availablePartitions = new ArrayList<>();
            for (PartitionInfo part : partitionList) {
                if (part.leader() != null)
                    availablePartitions.add(part);
            }
            // 将主题与其可用分区信息记录到partitionsByTopic
            this.availablePartitionsByTopic.put(topic, Collections.unmodifiableList(availablePartitions));
        }
        // 封装partitionsByNode
        this.partitionsByNode = new HashMap<>(partsForNode.size());
        for (Map.Entry<Integer, List<PartitionInfo>> entry : partsForNode.entrySet())
            this.partitionsByNode.put(entry.getKey(), Collections.unmodifiableList(entry.getValue()));
        // 封装unauthorizedTopics
        this.unauthorizedTopics = Collections.unmodifiableSet(unauthorizedTopics);
    }

    /**
     * Create an empty cluster instance with no nodes and no topic-partitions.
     */
    public static Cluster empty() {
        return new Cluster(new ArrayList<Node>(0), new ArrayList<PartitionInfo>(0), Collections.<String>emptySet());
    }

    /**
     * Create a "bootstrap" cluster using the given list of host/ports
     * 基于InetSocketAddress列表，封装一个Cluster对象
     * @param addresses The addresses
     * @return A cluster for these hosts/ports
     */
    public static Cluster bootstrap(List<InetSocketAddress> addresses) {
        List<Node> nodes = new ArrayList<>();
        int nodeId = -1;
        for (InetSocketAddress address : addresses)
            nodes.add(new Node(nodeId--, address.getHostString(), address.getPort()));
        return new Cluster(true, nodes, new ArrayList<PartitionInfo>(0), Collections.<String>emptySet());
    }

    /**
     * Return a copy of this cluster combined with `partitions`.
     */
    public Cluster withPartitions(Map<TopicPartition, PartitionInfo> partitions) {
        Map<TopicPartition, PartitionInfo> combinedPartitions = new HashMap<>(this.partitionsByTopicPartition);
        combinedPartitions.putAll(partitions);
        return new Cluster(this.nodes, combinedPartitions.values(), new HashSet<>(this.unauthorizedTopics));
    }

    /**
     * @return The known set of nodes
     */
    public List<Node> nodes() {
        return this.nodes;
    }
    
    /**
     * Get the node by the node id (or null if no such node exists)
     * @param id The id of the node
     * @return The node, or null if no such node exists
     */
    public Node nodeById(int id) {
        return this.nodesById.get(id);
    }

    /**
     * Get the current leader for the given topic-partition
     * @param topicPartition The topic and partition we want to know the leader for
     * @return The node that is the leader for this topic-partition, or null if there is currently no leader
     */
    public Node leaderFor(TopicPartition topicPartition) {
        PartitionInfo info = partitionsByTopicPartition.get(topicPartition);
        if (info == null)
            return null;
        else
            return info.leader();
    }

    /**
     * Get the metadata for the specified partition
     * @param topicPartition The topic and partition to fetch info for
     * @return The metadata about the given topic and partition
     */
    public PartitionInfo partition(TopicPartition topicPartition) {
        return partitionsByTopicPartition.get(topicPartition);
    }

    /**
     * Get the list of partitions for this topic
     * @param topic The topic name
     * @return A list of partitions
     */
    public List<PartitionInfo> partitionsForTopic(String topic) {
        return this.partitionsByTopic.get(topic);
    }

    /**
     * Get the list of available partitions for this topic
     * @param topic The topic name
     * @return A list of partitions
     */
    public List<PartitionInfo> availablePartitionsForTopic(String topic) {
        return this.availablePartitionsByTopic.get(topic);
    }

    /**
     * Get the list of partitions whose leader is this node
     * @param nodeId The node id
     * @return A list of partitions
     */
    public List<PartitionInfo> partitionsForNode(int nodeId) {
        return this.partitionsByNode.get(nodeId);
    }

    /**
     * Get the number of partitions for the given topic
     * @param topic The topic to get the number of partitions for
     * @return The number of partitions or null if there is no corresponding metadata
     */
    public Integer partitionCountForTopic(String topic) {
        List<PartitionInfo> partitionInfos = this.partitionsByTopic.get(topic);
        return partitionInfos == null ? null : partitionInfos.size();
    }

    /**
     * Get all topics.
     * @return a set of all topics
     */
    public Set<String> topics() {
        return this.partitionsByTopic.keySet();
    }

    public Set<String> unauthorizedTopics() {
        return unauthorizedTopics;
    }

    public boolean isBootstrapConfigured() {
        return isBootstrapConfigured;
    }

    @Override
    public String toString() {
        return "Cluster(nodes = " + this.nodes + ", partitions = " + this.partitionsByTopicPartition.values() + ")";
    }

}
