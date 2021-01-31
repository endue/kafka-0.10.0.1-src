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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.CircularIterator;
import org.apache.kafka.common.utils.Utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * The roundrobin assignor lays out all the available partitions and all the available consumers. It
 * then proceeds to do a roundrobin assignment from partition to consumer. If the subscriptions of all consumer
 * instances are identical, then the partitions will be uniformly distributed. (i.e., the partition ownership counts
 * will be within a delta of exactly one across all consumers.)
 *
 * For example, suppose there are two consumers C0 and C1, two topics t0 and t1, and each topic has 3 partitions,
 * resulting in partitions t0p0, t0p1, t0p2, t1p0, t1p1, and t1p2.
 *
 * The assignment will be:
 * C0: [t0p0, t0p2, t1p1]
 * C1: [t0p1, t1p0, t1p2]
 */
public class RoundRobinAssignor extends AbstractPartitionAssignor {

    /**
     * 分配分区
     * 逻辑就是先将每个topic中的分区转换为TopicPartition对象，存放到一个List<TopicPartition>集合中
     * 然后遍历List<TopicPartition>集合，每次遍历就是取出一个topic-partition，然后遍历consumer，获取订阅该topic的consumer，
     * 将当前topic-partition交给这个consumer
     * @param partitionsPerTopic The number of partitions for each subscribed topic. Topics not in metadata will be excluded
     *                           from this map.
     * @param subscriptions Map from the memberId to their respective topic subscription
     * @return
     */
    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,// topic和对应的分区数
                                                    Map<String, List<String>> subscriptions) {// memberId和订阅的topics
        // 记录分配结果
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        for (String memberId : subscriptions.keySet())
            assignment.put(memberId, new ArrayList<TopicPartition>());
        // 根据memberId排序
        CircularIterator<String> assigner = new CircularIterator<>(Utils.sorted(subscriptions.keySet()));
        // 所有topic的每个分区对应的TopicPartition对象
        for (TopicPartition partition : allPartitionsSorted(partitionsPerTopic, subscriptions)) {
            final String topic = partition.topic();
            // 如果当前遍历的consumer没有定义该topic
            // 那么获取下一个consumer，直到获取到订阅当前topic的consumer
            while (!subscriptions.get(assigner.peek()).contains(topic))
                assigner.next();
            // 记录分配的结果
            assignment.get(assigner.next()).add(partition);
        }
        return assignment;
    }

    /**
     *
     * @param partitionsPerTopic topic和对应的分区数
     * @param subscriptions memberId和订阅的topics
     * @return
     */
    public List<TopicPartition> allPartitionsSorted(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, List<String>> subscriptions) {
        // 记录所有的topics
        SortedSet<String> topics = new TreeSet<>();
        for (List<String> subscription : subscriptions.values())
            topics.addAll(subscription);
        // 记录所有topic的每个分区对应的TopicPartition对象
        List<TopicPartition> allPartitions = new ArrayList<>();
        for (String topic : topics) {
            Integer numPartitionsForTopic = partitionsPerTopic.get(topic);
            if (numPartitionsForTopic != null)
                allPartitions.addAll(AbstractPartitionAssignor.partitions(topic, numPartitionsForTopic));
        }
        return allPartitions;
    }

    @Override
    public String name() {
        return "roundrobin";
    }

}
