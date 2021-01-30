/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The range assignor works on a per-topic basis. For each topic, we lay out the available partitions in numeric order
 * and the consumers in lexicographic order. We then divide the number of partitions by the total number of
 * consumers to determine the number of partitions to assign to each consumer. If it does not evenly
 * divide, then the first few consumers will have one extra partition.
 *
 * For example, suppose there are two consumers C0 and C1, two topics t0 and t1, and each topic has 3 partitions,
 * resulting in partitions t0p0, t0p1, t0p2, t1p0, t1p1, and t1p2.
 *
 * The assignment will be:
 * C0: [t0p0, t0p1, t1p0, t1p1]
 * C1: [t0p2, t1p2]
 */
public class RangeAssignor extends AbstractPartitionAssignor {

    @Override
    public String name() {
        return "range";
    }

    // 参数为memberId和订阅的topics
    // 转换参数Map<MemberId, List<Topic>>为Map<Topic, List<MemberId>>
    private Map<String, List<String>> consumersPerTopic(Map<String, List<String>> consumerMetadata) {
        Map<String, List<String>> res = new HashMap<>();
        for (Map.Entry<String, List<String>> subscriptionEntry : consumerMetadata.entrySet()) {
            String consumerId = subscriptionEntry.getKey();
            for (String topic : subscriptionEntry.getValue())
                put(res, topic, consumerId);
        }
        return res;
    }

    // 分配分区
    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,// topic和对应的分区数
                                                    Map<String, List<String>> subscriptions) {// memberId和订阅的topics
        Map<String, List<String>> consumersPerTopic = consumersPerTopic(subscriptions);
        // 初始化assignment用来记录分配结果
        // 也就是每个member和对应分配的topic-partiton
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        for (String memberId : subscriptions.keySet())
            assignment.put(memberId, new ArrayList<TopicPartition>());
        // 遍历出每个topic的所有订阅者
        for (Map.Entry<String, List<String>> topicEntry : consumersPerTopic.entrySet()) {
            String topic = topicEntry.getKey();
            // 获取当前topic的所有consumer，假设3
            List<String> consumersForTopic = topicEntry.getValue();
            // 获取当前topic的分区数，假设10
            Integer numPartitionsForTopic = partitionsPerTopic.get(topic);
            if (numPartitionsForTopic == null)
                continue;
            // 排序
            Collections.sort(consumersForTopic);
            // 计算每个consumer需要被分配的分区数,10 / 3 = 3
            int numPartitionsPerConsumer = numPartitionsForTopic / consumersForTopic.size();
            // 计算余数 10 % 3 = 1
            int consumersWithExtraPartition = numPartitionsForTopic % consumersForTopic.size();
            // 基于numPartitionsPerConsumer创建List<TopicPartition>
            List<TopicPartition> partitions = AbstractPartitionAssignor.partitions(topic, numPartitionsForTopic);
            // 遍历consumer的数量，计算每个consumer负责的分区
            for (int i = 0, n = consumersForTopic.size(); i < n; i++) {
                // consumer1 : start = 0，length = 4 [0,4)
                // consumer2 : start = 4, length = 3 [4,7)
                // consumer3 : start = 7,length = 3 [7,10)
                int start = numPartitionsPerConsumer * i + Math.min(i, consumersWithExtraPartition);
                int length = numPartitionsPerConsumer + (i + 1 > consumersWithExtraPartition ? 0 : 1);
                assignment.get(consumersForTopic.get(i)).addAll(partitions.subList(start, start + length));
            }
        }
        return assignment;
    }

}
