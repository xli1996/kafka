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

package kafka.server.metadata

import kafka.coordinator.group.GroupCoordinator
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.server.{KafkaConfig, MetadataCache, QuotaFactory, ReplicaManager}
import kafka.utils.Logging
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.protocol.ApiMessage

/**
 * Ability to process ApiMessage
 */
trait ApiMessageProcessor {
  /**
   * Process a list of messages
   *
   * @param apiMessages the messages to process
   */
  def process(apiMessages: List[ApiMessage]): Unit

  /**
   * Process a register-local-broker event from the broker's heartbeat
   *
   * @param registerLocalBrokerEvent the event to process
   */
  def process(registerLocalBrokerEvent: OutOfBandRegisterLocalBrokerEvent): Unit

  /**
   * Process a fence-local-broker event from the broker's heartbeat
   *
   * @param fencerLocalBrokerEvent the event to process
   */
  def process(fencerLocalBrokerEvent: OutOfBandFenceLocalBrokerEvent): Unit
}

object BrokerMetadataProcessor {
  // visible for testing
  @volatile private[metadata] var currentMetadataOffset: Long = -1

  def defaultProcessors(kafkaConfig: KafkaConfig,
                        clusterId: String,
                        metadataCache: MetadataCache,
                        groupCoordinator: GroupCoordinator,
                        quotaManagers: QuotaFactory.QuotaManagers,
                        replicaManager: ReplicaManager,
                        txnCoordinator: TransactionCoordinator): List[ApiMessageProcessor] = {
    List(new PartitionMetadataProcessor(kafkaConfig, clusterId, metadataCache, groupCoordinator, quotaManagers,
      replicaManager, txnCoordinator))
  }
}

class BrokerMetadataProcessor(processors: List[ApiMessageProcessor])
  extends BrokerMetadataEventProcessor with Logging {
  if (processors.isEmpty) {
    throw new IllegalArgumentException(s"Empty ApiMessageProcessor list!")
  }

  override def currentMetadataOffset(): Long = BrokerMetadataProcessor.currentMetadataOffset

  override def processStartup(): Unit = {}

  override def process(apiMessages: List[ApiMessage], lastMetadataOffset: Long): Unit = {
    try {
      if (isTraceEnabled) {
        trace(s"Handling metadata messages: $apiMessages")
      }
      // check to make sure the metadata offset makes sense
      val currentOffset = currentMetadataOffset()
      if (lastMetadataOffset < currentOffset + apiMessages.size) {
        throw new IllegalArgumentException(s"Metadata offset of last message in batch of size ${apiMessages.size}" +
          s" is too small for current metadata offset $currentOffset: $lastMetadataOffset")
      }
      // Give each processor an opportunity to process the batch.

      // We could introduce queues, run each processor in a separate thread, and pipeline message batches to them.
      // If we pipeline, then we would probably add a CompletableFuture to the ApiMessageProcessor.process() methods
      // and we would have a separate thread/queue waiting on each batch's futures to complete before applying the
      // corresponding metadata offset.

      processors.foreach(processor =>
        try {
          processor.process(apiMessages) // synchronous, see above for an alternative
        } catch {
          case e: FatalExitError => throw e
          case e: Exception => error(s"Error when processor $processor handled metadata messages: $apiMessages", e)
        })
      // set the new offset now that all processors have processed the batch
      if (isTraceEnabled) {
        trace(s"Setting current metadata offset to $lastMetadataOffset after handling metadata messages: $apiMessages")
      }
      BrokerMetadataProcessor.currentMetadataOffset = lastMetadataOffset
    } catch {
      case e: FatalExitError => throw e
      case e: Exception => error(s"Error when handling metadata messages: $apiMessages", e)
    }
  }

  override def process(registerLocalBrokerEvent: OutOfBandRegisterLocalBrokerEvent): Unit = {
    try {
      if (isTraceEnabled) {
        trace(s"Handling broker heartbeat register-local-broker message: $registerLocalBrokerEvent")
      }
      processors.foreach(processor =>
        try {
          processor.process(registerLocalBrokerEvent) // synchronous, see above for an alternative
        } catch {
          case e: FatalExitError => throw e
          case e: Exception => error(s"Error when processor $processor handled broker heartbeat register-local-broker message: $registerLocalBrokerEvent", e)
        })
      if (isTraceEnabled) {
        trace(s"Handled broker heartbeat register-local-broker message: $registerLocalBrokerEvent")
      }
    } catch {
      case e: FatalExitError => throw e
      case e: Exception => error(s"Error when handling broker heartbeat register-local-broker message: $registerLocalBrokerEvent", e)
    }
  }

  override def process(fenceLocalBrokerEvent: OutOfBandFenceLocalBrokerEvent): Unit = {
    try {
      if (isTraceEnabled) {
        trace(s"Handling broker heartbeat fence-local-broker message: $fenceLocalBrokerEvent")
      }
      processors.foreach(processor =>
        try {
          processor.process(fenceLocalBrokerEvent) // synchronous, see above for an alternative
        } catch {
          case e: FatalExitError => throw e
          case e: Exception => error(s"Error when processor $processor handled broker heartbeat fence-local-broker message: $fenceLocalBrokerEvent", e)
        })
      if (isTraceEnabled) {
        trace(s"Handled broker heartbeat fence-local-broker message: $fenceLocalBrokerEvent")
      }
    } catch {
      case e: FatalExitError => throw e
      case e: Exception => error(s"Error when handling broker heartbeat fence-local-broker message: $fenceLocalBrokerEvent", e)
    }
  }
}
