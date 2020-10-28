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

import org.apache.kafka.common.protocol.ApiMessage
import org.junit.Assert.assertEquals
import org.junit.{Before, Test}
import org.mockito.Mockito.mock

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class BrokerMetadataProcessorTest {
  val initialMetadataOffset = -1
  val processor1Key = "processor1"
  val processor2Key = "processor2"
  val processorKeys = List(processor1Key, processor2Key)
  val apiMessageInvocations = mutable.Map[String, ListBuffer[List[ApiMessage]]]()
  val outOfBandRegisterLocalBrokerInvocations = mutable.Map[String, ListBuffer[OutOfBandRegisterLocalBrokerEvent]]()
  val outOfbandFenceLocalBrokerInvocations = mutable.Map[String, ListBuffer[OutOfBandFenceLocalBrokerEvent]]()

  @Before
  def setUp(): Unit = {
    processorKeys.foreach(key => {
      apiMessageInvocations.put(key, ListBuffer.empty)
      outOfBandRegisterLocalBrokerInvocations.put(key, ListBuffer.empty)
      outOfbandFenceLocalBrokerInvocations.put(key, ListBuffer.empty)
    })
    BrokerMetadataProcessor.currentMetadataOffset = initialMetadataOffset
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testEmptyApiMessageProcessors(): Unit = {
    new BrokerMetadataProcessor(List.empty)
  }

  @Test
  def testInitialAndSubsequentMetadataOffsets(): Unit = {
    val processor = new BrokerMetadataProcessor(allProcessorsCountInvocations(processorKeys))
    assertEquals(initialMetadataOffset, processor.currentMetadataOffset())

    val nextMetadataOffset = initialMetadataOffset + 2
    val msg0 = mock(classOf[ApiMessage])
    val msg1 = mock(classOf[ApiMessage])
    val apiMessages = List(msg0, msg1)
    processor.process(apiMessages, nextMetadataOffset)

    // offset should be updated
    assertEquals(nextMetadataOffset, processor.currentMetadataOffset())

    // record should be processed
    processorKeys.foreach(key => {
      assertEquals(1, apiMessageInvocations.get(key).get.size)
      assertEquals(apiMessageInvocations.get(key).get.head, apiMessages)
      assertEquals(0, outOfBandRegisterLocalBrokerInvocations.get(key).get.size)
      assertEquals(0, outOfbandFenceLocalBrokerInvocations.get(key).get.size)
    })
  }

  @Test
  def testOutOfBandHeartbeatMessages(): Unit = {
    val processor = new BrokerMetadataProcessor(allProcessorsCountInvocations(processorKeys))
    assertEquals(initialMetadataOffset, processor.currentMetadataOffset())

    val msg0 = OutOfBandRegisterLocalBrokerEvent(1)
    val msg1 = OutOfBandFenceLocalBrokerEvent(1)
    processor.process(msg0)
    processor.process(msg1)

    // offset should not be updated
    assertEquals(initialMetadataOffset, processor.currentMetadataOffset())

    // out-of0band records should be processed
    processorKeys.foreach(key => {
      assertEquals(0, apiMessageInvocations.get(key).get.size)
      assertEquals(1, outOfBandRegisterLocalBrokerInvocations.get(key).get.size)
      assertEquals(outOfBandRegisterLocalBrokerInvocations.get(key).get.head, msg0)
      assertEquals(1, outOfbandFenceLocalBrokerInvocations.get(key).get.size)
      assertEquals(outOfbandFenceLocalBrokerInvocations.get(key).get.head, msg1)
    })
  }

  @Test
  def testBadMetadataOffsets(): Unit = {
    val processor = new BrokerMetadataProcessor(allProcessorsCountInvocations(processorKeys))

    val nextMetadataOffset = processor.currentMetadataOffset() - 1 // too low
    val msg0 = mock(classOf[ApiMessage])
    val msg1 = mock(classOf[ApiMessage])
    val apiMessages = List(msg0, msg1)
    processor.process(apiMessages, nextMetadataOffset)

    // offset should be unchanged
    assertEquals(initialMetadataOffset, processor.currentMetadataOffset())

    // record should not be processed
    processorKeys.foreach(key => {
      assertEquals(0, apiMessageInvocations.get(key).get.size)
      assertEquals(0, outOfBandRegisterLocalBrokerInvocations.get(key).get.size)
      assertEquals(0, outOfbandFenceLocalBrokerInvocations.get(key).get.size)
    })
  }

  def allProcessorsCountInvocations(processorKeys: List[String]): List[ApiMessageProcessor] = {
    processorKeys.map(key => new ApiMessageProcessor {
      override def process(apiMessages: List[ApiMessage]): Unit = {
        apiMessageInvocations.get(key).get += apiMessages
      }

      override def process(registerLocalBrokerEvent: OutOfBandRegisterLocalBrokerEvent): Unit = {
        outOfBandRegisterLocalBrokerInvocations.get(key).get += registerLocalBrokerEvent
      }

      override def process(fencerLocalBrokerEvent: OutOfBandFenceLocalBrokerEvent): Unit = {
        outOfbandFenceLocalBrokerInvocations.get(key).get += fencerLocalBrokerEvent
      }
    })
  }
}
