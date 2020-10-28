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

package kafka.server.metadata

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

import com.yammer.metrics.core.{Histogram, MetricName}
import kafka.metrics.KafkaYammerMetrics
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.common.protocol.ApiMessage
import org.apache.kafka.common.utils.MockTime
import org.junit.Assert.{assertEquals, assertTrue, fail}
import org.junit.{After, Test}
import org.mockito.Mockito.mock

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class BrokerMetadataListenerTest {
  val expectedMetricMBeanPrefix = "kafka.server.metadata:type=BrokerMetadataEventManager"
  val expectedEventQueueTimeMsMetricName = "EventQueueTimeMs"
  var brokerMetadataListener: BrokerMetadataListener = _

  @After
  def tearDown(): Unit = {
    if (brokerMetadataListener != null)
      brokerMetadataListener.close()
  }

  @Test
  def testMetricsCleanedOnClose(): Unit = {
    def allEventManagerMetrics: Set[MetricName] = {
      KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.keySet
        .filter(_.getMBeanName.startsWith(expectedMetricMBeanPrefix))
        .toSet
    }

    val time = new MockTime()
    brokerMetadataListener = new BrokerMetadataListener(mock(classOf[KafkaConfig]), time, mock(classOf[BrokerMetadataProcessor]))
    brokerMetadataListener.start()
    assertTrue(allEventManagerMetrics.nonEmpty)

    brokerMetadataListener.close()
    assertTrue(allEventManagerMetrics.isEmpty)
  }

  @Test
  def testEventIsProcessed(): Unit = {
    val time = new MockTime()
    val processedEvents = mutable.Buffer.empty[List[ApiMessage]]
    val startupEvent = List(mock(classOf[ApiMessage]), mock(classOf[ApiMessage]))

    val brokerMetadataProcessor = new BrokerMetadataEventProcessor {
      override def currentMetadataOffset(): Long = -1
      override def processStartup(): Unit = {
        processedEvents += startupEvent
      }
      override def process(apiMessages: List[ApiMessage], lastMetadataOffset: Long): Unit = {
        processedEvents += apiMessages
      }
      def process(fenceLocalBrokerEvent: kafka.server.metadata.OutOfBandFenceLocalBrokerEvent): Unit = {}
      def process(registerLocalBrokerEvent: kafka.server.metadata.OutOfBandRegisterLocalBrokerEvent): Unit = {}
    }

    brokerMetadataListener = new BrokerMetadataListener(mock(classOf[KafkaConfig]), time, brokerMetadataProcessor)
    brokerMetadataListener.start()

    val apiMessagesEvent = List(mock(classOf[ApiMessage]))
    brokerMetadataListener.put(MetadataEvent(apiMessagesEvent, 1))
    TestUtils.waitUntilTrue(() => processedEvents.size == 2,
      "Failed to process expected event before timing out")
    assertEquals(startupEvent, processedEvents(0))
    assertEquals(apiMessagesEvent, processedEvents(1))
  }

  @Test
  def testOutOfBandEventIsProcessed(): Unit = {
    val time = new MockTime()
    val processedEvents = mutable.Buffer.empty[List[ApiMessage]]
    val outOfBandRegisterLocalBrokerEvent = List(mock(classOf[ApiMessage]), mock(classOf[ApiMessage]))
    val outOfBandFenceLocalBrokerEvent = List(mock(classOf[ApiMessage]), mock(classOf[ApiMessage]), mock(classOf[ApiMessage]))
    val countDownLatch = new CountDownLatch(1)

    val brokerMetadataProcessor = new BrokerMetadataEventProcessor {
      override def currentMetadataOffset(): Long = -1
      override def processStartup(): Unit = {}
      override def process(apiMessages: List[ApiMessage], lastMetadataOffset: Long): Unit = {
        processedEvents += apiMessages
        countDownLatch.await()
      }

      override def process(registerLocalBrokerEvent: OutOfBandRegisterLocalBrokerEvent): Unit = {
        processedEvents += outOfBandRegisterLocalBrokerEvent
      }

      override def process(fenceLocalBrokerEvent: OutOfBandFenceLocalBrokerEvent): Unit = {
        processedEvents += outOfBandFenceLocalBrokerEvent
      }
    }

    brokerMetadataListener = new BrokerMetadataListener(mock(classOf[KafkaConfig]), time, brokerMetadataProcessor)
    brokerMetadataListener.start()

    val apiMessagesEvent1 = List(mock(classOf[ApiMessage]))
    val apiMessagesEvent2 = List(mock(classOf[ApiMessage]))
    // add the out-of-band messages after the batches
    brokerMetadataListener.put(MetadataEvent(apiMessagesEvent1, 1))
    brokerMetadataListener.put(MetadataEvent(apiMessagesEvent2, 1))
    brokerMetadataListener.put(OutOfBandRegisterLocalBrokerEvent(1))
    brokerMetadataListener.put(OutOfBandFenceLocalBrokerEvent(1))
    countDownLatch.countDown()
    TestUtils.waitUntilTrue(() => processedEvents.size == 4,
      "Failed to process expected event before timing out")
    // make sure out-of-band messages processed before the second batch
    assertEquals(apiMessagesEvent1, processedEvents(0))
    assertEquals(outOfBandRegisterLocalBrokerEvent, processedEvents(1))
    assertEquals(outOfBandFenceLocalBrokerEvent, processedEvents(2))
    assertEquals(apiMessagesEvent2, processedEvents(3))
  }

  @Test
  def testEventQueueTime(): Unit = {
    val metricName = s"$expectedMetricMBeanPrefix,name=$expectedEventQueueTimeMsMetricName"
    val time = new MockTime()
    val latch = new CountDownLatch(1)
    val processedEvents = new AtomicInteger()

    val brokerMetadataProcessor = new BrokerMetadataEventProcessor {
      override def currentMetadataOffset(): Long = -1
      override def processStartup(): Unit = {}
      override def process(apiMessages: List[ApiMessage], lastMetadataOffset: Long): Unit = {
        latch.await()
        time.sleep(500)
        processedEvents.incrementAndGet()
      }
      def process(fenceLocalBrokerEvent: kafka.server.metadata.OutOfBandFenceLocalBrokerEvent): Unit = {}
      def process(registerLocalBrokerEvent: kafka.server.metadata.OutOfBandRegisterLocalBrokerEvent): Unit = {}
    }

    // The metric should not already exist
    assertTrue(KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.filter { case (k, _) =>
      k.getMBeanName == metricName
    }.values.isEmpty)

    brokerMetadataListener = new BrokerMetadataListener(mock(classOf[KafkaConfig]), time, brokerMetadataProcessor)
    brokerMetadataListener.start()

    val apiMessagesEvent = List(mock(classOf[ApiMessage]))
    brokerMetadataListener.put(MetadataEvent(apiMessagesEvent, 1))
    brokerMetadataListener.put(MetadataEvent(apiMessagesEvent, 2))
    latch.countDown()

    TestUtils.waitUntilTrue(() => processedEvents.get() == 2,
      "Timed out waiting for processing of all events")

    val queueTimeHistogram = KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.filter { case (k, _) =>
      k.getMBeanName == metricName
    }.values.headOption.getOrElse(fail(s"Unable to find metric $metricName")).asInstanceOf[Histogram]

    assertEquals(2, queueTimeHistogram.count)
    assertEquals(0, queueTimeHistogram.min, 0.01)
    assertEquals(500, queueTimeHistogram.max, 0.01)
  }

  @Test
  def testEventQueueTimeResetOnTimeout(): Unit = {
    val metricName = s"$expectedMetricMBeanPrefix,name=$expectedEventQueueTimeMsMetricName"
    val time = new MockTime()
    val processedEvents = new AtomicInteger()

    val brokerMetadataProcessor = new BrokerMetadataEventProcessor {
      override def currentMetadataOffset(): Long = -1
      override def processStartup(): Unit = {}
      override def process(apiMessages: List[ApiMessage], lastMetadataOffset: Long): Unit = {
        processedEvents.incrementAndGet()
      }
      def process(fenceLocalBrokerEvent: kafka.server.metadata.OutOfBandFenceLocalBrokerEvent): Unit = {}
      def process(registerLocalBrokerEvent: kafka.server.metadata.OutOfBandRegisterLocalBrokerEvent): Unit = {}
    }

    brokerMetadataListener = new BrokerMetadataListener(mock(classOf[KafkaConfig]), time, brokerMetadataProcessor,
    1) // set short timeout so we don't have to wait too long for success
    brokerMetadataListener.start()

    val apiMessagesEvent = List(mock(classOf[ApiMessage]))
    brokerMetadataListener.put(MetadataEvent(apiMessagesEvent, 1))
    brokerMetadataListener.put(MetadataEvent(apiMessagesEvent, 2))

    TestUtils.waitUntilTrue(() => processedEvents.get() == 2,
      "Timed out waiting for processing of all events")

    val queueTimeHistogram = KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.filter { case (k, _) =>
      k.getMBeanName == metricName
    }.values.headOption.getOrElse(fail(s"Unable to find metric $metricName")).asInstanceOf[Histogram]
    // we know from previous tests that the histogram count at this point will be 2

    // wait for the timeout/reset
    TestUtils.waitUntilTrue(() => queueTimeHistogram.count == 0,
      s"Timed out on resetting $expectedEventQueueTimeMsMetricName Histogram")
    assertEquals(0, queueTimeHistogram.min, 0.1)
    assertEquals(0, queueTimeHistogram.max, 0.1)
  }
}
