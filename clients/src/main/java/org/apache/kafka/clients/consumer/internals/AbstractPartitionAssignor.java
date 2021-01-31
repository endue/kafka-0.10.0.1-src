/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Abstract assignor implementation which does some common grunt work (in particular collecting
 * partition counts which are always needed in assignors).
 */
public abstract class AbstractPartitionAssignor implements PartitionAssignor {
    private static final Logger log = LoggerFactory.getLogger(AbstractPartitionAssignor.class);

    /**
     * Perform the group assignment given the partition counts and member subscriptions
     * @param partitionsPerTopic The number of partitions for each subscribed topic. Topics not in metadata will be excluded
     *                           from this map.
     * @param subscriptions Map from the memberId to their respective topic subscription
     * @return Map from each member to the list of partitions assigned to them.
     */
    // 分配分区
    // partitionsPerTopic 每个topic的分区数
    // subscriptions memberId和其订阅的所有topic
    public abstract Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                             Map<String, List<String>> subscriptions);

    @Override
    public Subscription subscription(Set<String> topics) {
        return new Subscription(new ArrayList<>(topics));
    }

    /**
     * 分配分区，返回结果key是memberId，value是Assignment，Assignment里面记录了分配结果
     * @param metadata Current topic/broker metadata known by consumer
     * @param subscriptions Subscriptions from all members provided through {@link #subscription(Set)} 成员Id和对应的Subscription
     * @return
     */
    @Override
    public Map<String, Assignment> assign(Cluster metadata, Map<String, Subscription> subscriptions) {
        // 记录当前组成员订阅的所有topic
        Set<String> allSubscribedTopics = new HashSet<>();
        // 记录当前组成员和其订阅的所有topic
        Map<String, List<String>> topicSubscriptions = new HashMap<>();
        // 遍历参数解析结果保存到上述两个集合中
        for (Map.Entry<String, Subscription> subscriptionEntry : subscriptions.entrySet()) {
            // 获取某成员订阅的所有topic
            List<String> topics = subscriptionEntry.getValue().topics();
            // 记录到集合中
            allSubscribedTopics.addAll(topics);
            topicSubscriptions.put(subscriptionEntry.getKey(), topics);
        }

        // 记录当前订阅的所有topic的其分区数
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        // 遍历组成员订阅的所有topic
        for (String topic : allSubscribedTopics) {
            // 获取topic的分区数
            Integer numPartitions = metadata.partitionCountForTopic(topic);
            if (numPartitions != null && numPartitions > 0)
                partitionsPerTopic.put(topic, numPartitions);
            else
                log.debug("Skipping assignment for topic {} since no metadata is available", topic);
        }
        // 分配分区
        // 返回结果key是memberId，value是分配的topic-partition集合
        Map<String, List<TopicPartition>> rawAssignments = assign(partitionsPerTopic, topicSubscriptions);

        // this class has maintains no user data, so just wrap the results
        // 封装返回结果，将上述分配结果中的List<TopicPartition>封装为Assignment
        Map<String, Assignment> assignments = new HashMap<>();
        //
        for (Map.Entry<String, List<TopicPartition>> assignmentEntry : rawAssignments.entrySet())
            assignments.put(assignmentEntry.getKey(), new Assignment(assignmentEntry.getValue()));
        // 返回分配的topic-partition
        return assignments;
    }

    @Override
    public void onAssignment(Assignment assignment) {
        // this assignor maintains no internal state, so nothing to do
    }

    // 工具方法
    // 将value记录到map中对应key的list中
    protected static <K, V> void put(Map<K, List<V>> map, K key, V value) {
        List<V> list = map.get(key);
        if (list == null) {
            list = new ArrayList<>();
            map.put(key, list);
        }
        list.add(value);
    }
    // 根据topic的分区数，创建对应的List<TopicPartition>集合
    protected static List<TopicPartition> partitions(String topic, int numPartitions) {
        List<TopicPartition> partitions = new ArrayList<>(numPartitions);
        for (int i = 0; i < numPartitions; i++)
            partitions.add(new TopicPartition(topic, i));
        return partitions;
    }
}
