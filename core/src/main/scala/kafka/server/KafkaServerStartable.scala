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

import java.util.Properties

import kafka.metrics.KafkaMetricsReporter
import kafka.utils.{VerifiableProperties, Logging}

object KafkaServerStartable {
  def fromProps(serverProps: Properties) = {
    KafkaMetricsReporter.startReporters(new VerifiableProperties(serverProps))
    new KafkaServerStartable(KafkaConfig.fromProps(serverProps))
  }
}

class KafkaServerStartable(val serverConfig: KafkaConfig) extends Logging {
  // 基于配置构建一个KafkaServer
  private val server = new KafkaServer(serverConfig)

  // 启动kafkaServer
  def startup() {
    try {
      server.startup()
    }
    catch {
      case e: Throwable =>
        fatal("Fatal error during KafkaServerStartable startup. Prepare to shutdown", e)
        // KafkaServer already calls shutdown() internally, so this is purely for logging & the exit code
        System.exit(1)
    }
  }

  // 关闭kafkaServer
  def shutdown() {
    try {
      server.shutdown()
    }
    catch {
      case e: Throwable =>
        fatal("Fatal error during KafkaServerStable shutdown. Prepare to halt", e)
        // Calling exit() can lead to deadlock as exit() can be called multiple times. Force exit.
        Runtime.getRuntime.halt(1)
    }
  }

  /**
   * Allow setting broker state from the startable.
   * This is needed when a custom kafka server startable want to emit new states that it introduces.
   */
  // 设置kafkaServer状态
  def setServerState(newState: Byte) {
    server.brokerState.newState(newState)
  }

  // kafkaServer启动调用该方法阻塞
  def awaitShutdown() = 
    server.awaitShutdown

}


