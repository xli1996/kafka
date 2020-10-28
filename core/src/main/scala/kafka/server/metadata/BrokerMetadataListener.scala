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

import java.util
import java.util.concurrent.{ConcurrentLinkedQueue, LinkedBlockingQueue, TimeUnit}

import kafka.metrics.KafkaMetricsGroup
import kafka.server.KafkaConfig
import kafka.utils.ShutdownableThread
import org.apache.kafka.common.protocol.ApiMessage
import org.apache.kafka.common.utils.Time

object BrokerMetadataListener {
  val ThreadNamePrefix = "broker-"
  val ThreadNameSuffix = "-metadata-event-thread"
  val EventQueueTimeMetricName = "EventQueueTimeMs"
  val EventQueueSizeMetricName = "EventQueueSize"
}

sealed trait BrokerMetadataEvent {}

final case object StartupEvent extends BrokerMetadataEvent

/**
 * A batch of messages from the metadata log
 *
 * @param apiMessages the batch of messages
 * @param lastMetadataOffset the metadata offset of the last message in the batch
 */
final case class MetadataEvent(apiMessages: List[ApiMessage], lastMetadataOffset: Long) extends BrokerMetadataEvent

/**
 * An event that occurs when the broker heartbeat receives a successful registration response.
 * It will only occur once in the lifetime of the broker process, and it will occur before
 * any metadata log message batches appear.  The manager injects this event into the event stream,
 * and processors will receive it immediately.
 *
 * @param brokerEpoch the epoch assigned to the broker by the active controller
 */
final case class OutOfBandRegisterLocalBrokerEvent(brokerEpoch: Long) extends BrokerMetadataEvent

/**
 * An event that occurs when either:
 *
 * 1) the local broker's heartbeat is unable to contact the active controller within the
 * defined lease duration and loses its lease
 * 2) the local broker is told by the controller that it should be in the fenced state
 *
 * The manager injects this event into the event stream such that processors receive it as soon as they finish their
 * current message batch, if any, otherwise they receive it immediately.
 *
 * @param brokerEpoch the broker epoch that was fenced
 */
final case class OutOfBandFenceLocalBrokerEvent(brokerEpoch: Long) extends BrokerMetadataEvent

/*
 * WakeupEvent is necessary for the case when the event thread is blocked in queue.take() and we need to wake it up.
 * This event has no other semantic meaning and should be ignored when seen.  It is useful in two specific situations:
 * 1) when an out-of-band message is available; and 2) when shutdown needs to occur.  The presence of this message
 * ensures these other messages are seen quickly when no other messages are in the queue.
 */
final case object WakeupEvent extends BrokerMetadataEvent

class QueuedEvent(val event: BrokerMetadataEvent, val enqueueTimeMs: Long) {
  override def toString: String = {
    s"QueuedEvent(event=$event, enqueueTimeMs=$enqueueTimeMs)"
  }
}

trait MetadataEventProcessor {
  def currentMetadataOffset(): Long
  def processStartup(): Unit
  def process(apiMessages: List[ApiMessage], lastMetadataOffset: Long): Unit
  def process(registerLocalBrokerEvent: OutOfBandRegisterLocalBrokerEvent): Unit
  def process(fenceLocalBrokerEvent: OutOfBandFenceLocalBrokerEvent): Unit
}

class BrokerMetadataListener(config: KafkaConfig,
                             time: Time,
                             processor: MetadataEventProcessor,
                             eventQueueTimeHistogramTimeoutMs: Long = 300000) extends KafkaMetricsGroup {

  private val blockingQueue = new LinkedBlockingQueue[QueuedEvent]
  private val bufferQueue: util.Queue[QueuedEvent] = new util.ArrayDeque[QueuedEvent]
  private val outOfBandQueue = new ConcurrentLinkedQueue[QueuedEvent]
  @volatile private var totalQueueSize: Int = 0

  private val thread = new BrokerMetadataEventThread(
    s"${BrokerMetadataListener.ThreadNamePrefix}${config.brokerId}${BrokerMetadataListener.ThreadNameSuffix}")

  // metrics
  private val eventQueueTimeHist = newHistogram(BrokerMetadataListener.EventQueueTimeMetricName)
  newGauge(BrokerMetadataListener.EventQueueSizeMetricName, () => totalQueueSize)

  def start(): Unit = {
    put(StartupEvent)
    thread.start()
  }

  def close(): Unit = {
    try {
      thread.initiateShutdown()
      put(WakeupEvent) // wake up the thread in case it is blocked on queue.take()
      thread.awaitShutdown()
    } finally {
      removeMetric(BrokerMetadataListener.EventQueueTimeMetricName)
      removeMetric(BrokerMetadataListener.EventQueueSizeMetricName)
    }
  }

  def put(event: BrokerMetadataEvent): QueuedEvent = {
    val queuedEvent = new QueuedEvent(event, time.milliseconds())
    event match {
      case _: OutOfBandRegisterLocalBrokerEvent |
           _: OutOfBandFenceLocalBrokerEvent =>
        outOfBandQueue.add(queuedEvent)
        blockingQueue.add(new QueuedEvent(WakeupEvent, queuedEvent.enqueueTimeMs))
      case _ => blockingQueue.put(queuedEvent)
    }
    queuedEvent
  }

  def currentMetadataOffset(): Long = processor.currentMetadataOffset()

  private class BrokerMetadataEventThread(name: String) extends ShutdownableThread(name = name, isInterruptible = false) {
    logIdent = s"[BrokerMetadataEventThread] "

    override def doWork(): Unit = {
      val dequeued: QueuedEvent = pollFromEventQueue()
      dequeued.event match {
        case StartupEvent => processor.processStartup()
        case metadataEvent: MetadataEvent =>
          eventQueueTimeHist.update(time.milliseconds() - dequeued.enqueueTimeMs)
          try {
            processor.process(metadataEvent.apiMessages, metadataEvent.lastMetadataOffset)
          } catch {
            case e: Throwable => error(s"Uncaught error processing event: $metadataEvent", e)
          }
        case WakeupEvent => // Ignore since it serves solely to wake us up
        case registerLocalBrokerEvent: OutOfBandRegisterLocalBrokerEvent =>
          try {
            processor.process(registerLocalBrokerEvent)
          } catch {
            case e: Throwable => error(s"Uncaught error processing event: $registerLocalBrokerEvent", e)
          }
        case fenceLocalBrokerEvent: OutOfBandFenceLocalBrokerEvent =>
          try {
            processor.process(fenceLocalBrokerEvent)
          } catch {
            case e: Throwable => error(s"Uncaught error processing event: $fenceLocalBrokerEvent", e)
          }
      }
    }

    private def pollFromEventQueue(): QueuedEvent = {
      val outOfBandEvent = outOfBandQueue.poll()
      if (outOfBandEvent != null) {
        return outOfBandEvent
      }
      val bufferedEvent = bufferQueue.poll()
      if (bufferedEvent != null) {
        totalQueueSize = blockingQueue.size() + bufferQueue.size()
        return bufferedEvent
      }
      val numBuffered = blockingQueue.drainTo(bufferQueue)
      if (numBuffered != 0) {
        totalQueueSize = blockingQueue.size() + bufferQueue.size() - 1 // we're going to pull an event off and return it
        return bufferQueue.poll()
      }
      val hasRecordedValue = eventQueueTimeHist.count() > 0
      if (hasRecordedValue) {
        val event = blockingQueue.poll(eventQueueTimeHistogramTimeoutMs, TimeUnit.MILLISECONDS)
        if (event == null) {
          eventQueueTimeHist.clear()
          blockingQueue.take()
        } else {
          event
        }
      } else {
        blockingQueue.take()
      }
    }
  }
}
