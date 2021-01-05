/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import kafka.testkit.{KafkaClusterTestKit, TestKitNodes}
import org.apache.kafka.clients.admin.{Admin, NewTopic}
import org.junit.rules.Timeout
import org.junit.{Rule, Test}

import java.time.Duration
import java.util.Collections
import java.util.concurrent.TimeUnit

class Kip500ClusterTest {
  @Rule
  def globalTimeout = Timeout.millis(120000)

  @Test
  def testCreateClusterAndClose(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumKip500BrokerNodes(1).
        setNumControllerNodes(1).build()).build()
    try {
    } finally {
      cluster.close()
    }
  }

  @Test
  def testCreateTopic(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setNumKip500BrokerNodes(1).
        setNumControllerNodes(1).build()).build()
    try {
      cluster.format()
      cluster.startup()
      val adminClient = Admin.create(cluster.clientProperties())
      try {
        val newTopic = new NewTopic("test-topic", 1, 1.toShort)
        val createTopicResult = adminClient.createTopics(Collections.singletonList(newTopic))
        createTopicResult.all().get(10, TimeUnit.SECONDS)
      } finally {
        adminClient.close(Duration.ofSeconds(10))
      }
    } finally {
      cluster.close()
    }
  }
}
