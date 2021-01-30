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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This interface is used to define custom partition assignment for use in
 * 该接口用于定义自定义分区分配
 * {@link org.apache.kafka.clients.consumer.KafkaConsumer}. Members of the consumer group subscribe
 * to the topics they are interested in and forward their subscriptions to a Kafka broker serving
 * as the group coordinator. The coordinator selects one member to perform the group assignment and
 * propagates the subscriptions of all members to it. Then {@link #assign(Cluster, Map)} is called
 * to perform the assignment and the results are forwarded back to each respective members
 * 消费者组中的每个成员将他们定义的topic发送给GroupCoordinator,然后GroupCoordinator选择一个成员作为leader，
 * 然后将组成员的所有订阅信息发生给它，让它来执行分配任务。assign()方法就是来执行分配的。最后leader在把分区结果
 * 发送给GroupCoordinator，由GroupCoordinator转发给组内的每个成员
 *
 * In some cases, it is useful to forward additional metadata to the assignor in order to make
 * assignment decisions. For this, you can override {@link #subscription(Set)} and provide custom
 * userData in the returned Subscription. For example, to have a rack-aware assignor, an implementation
 * can use this user data to forward the rackId belonging to each member.
 * 在某些情况下，需要添加一些额外信息来决定分配的结果，这时，我们可以覆盖subscription()方法，在其返回的Subscription
 * 对象中增加一些额外的信息“userData”，比如，为了实现一个机架感知的Assignor，我们可以使用“userData”来转发每个组成员
 * 的“rackId”信息
 *
 */
public interface PartitionAssignor {

    /**
     * Return a serializable object representing the local member's subscription. This can include
     * additional information as well (e.g. local host/rack information) which can be leveraged in
     * {@link #assign(Cluster, Map)}.
     * @param topics Topics subscribed to through {@link org.apache.kafka.clients.consumer.KafkaConsumer#subscribe(java.util.Collection)}
     *               and variants
     * @return Non-null subscription with optional user data
     */
    // 创建Subscription
    Subscription subscription(Set<String> topics);

    /**
     * Perform the group assignment given the member subscriptions and current cluster metadata.
     * @param metadata Current topic/broker metadata known by consumer
     * @param subscriptions Subscriptions from all members provided through {@link #subscription(Set)}
     * @return A map from the members to their respective assignment. This should have one entry
     *         for all members who in the input subscription map.
     */
    // 分配分区
    Map<String, Assignment> assign(Cluster metadata, Map<String, Subscription> subscriptions);


    /**
     * Callback which is invoked when a group member receives its assignment from the leader.
     * @param assignment The local member's assignment as provided by the leader in {@link #assign(Cluster, Map)}
     */
    // 获得分配的分区信息
    void onAssignment(Assignment assignment);


    /**
     * Unique name for this assignor (e.g. "range" or "roundrobin")
     * @return non-null unique name
     */
    String name();

    // Subscription对象，包含Consumer订阅的topics和对应的额外信息
    class Subscription {
        private final List<String> topics;
        private final ByteBuffer userData;

        public Subscription(List<String> topics, ByteBuffer userData) {
            this.topics = topics;
            this.userData = userData;
        }

        public Subscription(List<String> topics) {
            this(topics, ByteBuffer.wrap(new byte[0]));
        }

        public List<String> topics() {
            return topics;
        }

        public ByteBuffer userData() {
            return userData;
        }

        @Override
        public String toString() {
            return "Subscription(" +
                    "topics=" + topics +
                    ')';
        }
    }

    // Assignment包含consumer被分配的topic-partition和额外信息
    class Assignment {
        private final List<TopicPartition> partitions;
        private final ByteBuffer userData;

        public Assignment(List<TopicPartition> partitions, ByteBuffer userData) {
            this.partitions = partitions;
            this.userData = userData;
        }

        public Assignment(List<TopicPartition> partitions) {
            this(partitions, ByteBuffer.wrap(new byte[0]));
        }

        public List<TopicPartition> partitions() {
            return partitions;
        }

        public ByteBuffer userData() {
            return userData;
        }

        @Override
        public String toString() {
            return "Assignment(" +
                    "partitions=" + partitions +
                    ')';
        }
    }

}
