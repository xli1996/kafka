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
import java.util.concurrent.TimeUnit
import kafka.coordinator.group.GroupCoordinator
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.log.LogManager
import kafka.metrics.KafkaMetricsGroup
import kafka.server.{MetadataCache, QuotaFactory, ReplicaManager, RequestHandlerHelper}
import kafka.utils.Logging
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.metadata.MetadataRecordType._
import org.apache.kafka.common.metadata._
import org.apache.kafka.common.protocol.ApiMessage
import org.apache.kafka.common.utils.{EventQueue, KafkaEventQueue, LogContext, Time}
import org.apache.kafka.metalog.{MetaLogLeader, MetaLogListener}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

object BrokerMetadataListener{
  val MetadataBatchProcessingTimeUs = "MetadataBatchProcessingTimeUs"
  val MetadataBatchSizes = "MetadataBatchSizes"
}

class BrokerMetadataListener(val brokerId: Int,
                             val time: Time,
                             val metadataCache: MetadataCache,
                             val configRepository: LocalConfigRepository,
                             val groupCoordinator: GroupCoordinator,
                             val quotaManagers: QuotaFactory.QuotaManagers,
                             val replicaManager: ReplicaManager,
                             val txnCoordinator: TransactionCoordinator,
                             val logManager: LogManager,
                             val threadNamePrefix: Option[String]
                            ) extends MetaLogListener with KafkaMetricsGroup with Logging {
  val logContext = new LogContext(s"[BrokerMetadataListener id=${brokerId}] ")

  this.logIdent = logContext.logPrefix()

  /**
   * A histogram tracking the time in microseconds it took to process batches of events.
   */
  private val batchProcessingTimeHist = newHistogram(BrokerMetadataListener.MetadataBatchProcessingTimeUs)

  /**
   * A histogram tracking the sizes of batches that we have processed.
   */
  private val metadataBatchSizeHist = newHistogram(BrokerMetadataListener.MetadataBatchSizes)

  /**
   * The highest metadata offset that we've seen.  Written only from the event queue thread.
   */
  @volatile private var _highestMetadataOffset = -1L

  val eventQueue = new KafkaEventQueue(time, logContext, threadNamePrefix.getOrElse(""))

  def highestMetadataOffset(): Long = _highestMetadataOffset

  var preRecoveryPartitionImageBuilder =
    Option(MetadataImageBuilder(brokerId, logger.underlying, metadataCache.currentImage()))
  var preRecoveryDeletedPartitions = Option(mutable.Buffer[MetadataPartition]())

  def handleLogRecoveryDone(): Unit = {
    eventQueue.append(new LogRecoveryDoneEvent())
  }

  class LogRecoveryDoneEvent()
    extends EventQueue.FailureLoggingEvent(logger.underlying) {
    override def run(): Unit = {
      preRecoveryPartitionImageBuilder.foreach(imageBuilder => applyPartitionChanges(imageBuilder, _highestMetadataOffset))
      preRecoveryPartitionImageBuilder = Option.empty
      preRecoveryDeletedPartitions.foreach(removedPartitions =>
        groupCoordinator.handleDeletedPartitions(removedPartitions.map(_.toTopicPartition()).toSeq))
      preRecoveryDeletedPartitions = Option.empty
    }
  }

  private def logRecoveryComplete = {
    preRecoveryPartitionImageBuilder.isEmpty
  }

  /**
   * Handle new metadata records.
   */
  override def handleCommits(lastOffset: Long, records: util.List[ApiMessage]): Unit = {
    eventQueue.append(new HandleCommitsEvent(lastOffset, records))
  }

  class HandleCommitsEvent(lastOffset: Long,
                           records: util.List[ApiMessage])
      extends EventQueue.FailureLoggingEvent(logger.underlying) {
    override def run(): Unit = {
      if (isDebugEnabled) {
        debug(s"Metadata batch ${lastOffset}: handling ${records.size()} record(s).")
      }
      val imageBuilder = MetadataImageBuilder(brokerId, logger.underlying, metadataCache.currentImage())
      val imageBuilders = Seq(Option(imageBuilder), preRecoveryPartitionImageBuilder)
      val startNs = time.nanoseconds()
      var index = 0
      metadataBatchSizeHist.update(records.size())
      records.iterator().asScala.foreach { case record =>
        try {
          if (isTraceEnabled) {
            trace("Metadata batch %d: processing [%d/%d]: %s.".format(lastOffset, index + 1,
              records.size(), record.toString()))
          }
          handleMessage(imageBuilders, record, lastOffset)
        } catch {
          case e: Exception => error(s"Unable to handle record ${index} in batch " +
            s"ending at offset ${lastOffset}", e)
        }
        index = index + 1
      }
      if (imageBuilder.hasChanges()) {
        val newImage = imageBuilder.build()
        if (isTraceEnabled) {
          trace(s"Metadata batch ${lastOffset}: creating new metadata image ${newImage}")
        } else if (isDebugEnabled) {
          debug(s"Metadata batch ${lastOffset}: creating new metadata image")
        }
        metadataCache.setImage(newImage)
      } else if (isDebugEnabled) {
        debug(s"Metadata batch ${lastOffset}: no new metadata image required.")
      }
      if (imageBuilder.hasPartitionChanges()) {
        if (logRecoveryComplete) {
          applyPartitionChanges(imageBuilder, lastOffset)
        } else {
          debug(s"Metadata batch ${lastOffset}: deferring partition changes until after log recovery")
        }
      } else if (isDebugEnabled) {
        debug(s"Metadata batch ${lastOffset}: no partition changes found.")
      }
      _highestMetadataOffset = lastOffset
      val endNs = time.nanoseconds()
      val deltaUs = TimeUnit.MICROSECONDS.convert(endNs - startNs, TimeUnit.NANOSECONDS)
      debug(s"Metadata batch ${lastOffset}: advanced highest metadata offset in ${deltaUs} " +
        "microseconds.")
      batchProcessingTimeHist.update(deltaUs)
    }
  }

  private def applyPartitionChanges(imageBuilder: MetadataImageBuilder, lastOffset: Long) = {
    if(isDebugEnabled) {
      debug(s"Metadata batch ${lastOffset}: applying partition changes")
    }
    val builder = imageBuilder.partitionsBuilder()
    val prevPartitions = imageBuilder.prevImage.partitions
    val changedPartitionsPreviouslyExisting = mutable.Set[MetadataPartition]()
    builder.localChanged().foreach(metadataPartition =>
      prevPartitions.get(metadataPartition.topicName, metadataPartition.partitionIndex).foreach(
        changedPartitionsPreviouslyExisting.add))
    replicaManager.handleMetadataRecords(lastOffset, builder.localChanged(),
      builder.localRemoved(), changedPartitionsPreviouslyExisting, imageBuilder.nextBrokers(),
      RequestHandlerHelper.onLeadershipChange(groupCoordinator, txnCoordinator, _, _))
  }

  private def handleMessage(imageBuilders: Seq[Option[MetadataImageBuilder]],
                            record: ApiMessage,
                            lastOffset: Long): Unit = {
    val recordType = try {
      fromId(record.apiKey())
    } catch {
      case e: Exception => throw new RuntimeException("Unknown metadata record type " +
      s"${record.apiKey()} in batch ending at offset ${lastOffset}.")
    }
    recordType match {
      case REGISTER_BROKER_RECORD => handleRegisterBrokerRecord(imageBuilders,
        record.asInstanceOf[RegisterBrokerRecord])
      case UNREGISTER_BROKER_RECORD => handleUnregisterBrokerRecord(imageBuilders,
        record.asInstanceOf[UnregisterBrokerRecord])
      case TOPIC_RECORD => handleTopicRecord(imageBuilders,
        record.asInstanceOf[TopicRecord])
      case PARTITION_RECORD => handlePartitionRecord(imageBuilders,
        record.asInstanceOf[PartitionRecord])
      case CONFIG_RECORD => handleConfigRecord(record.asInstanceOf[ConfigRecord])
      case ISR_CHANGE_RECORD => handleIsrChangeRecord(imageBuilders,
        record.asInstanceOf[IsrChangeRecord])
      case FENCE_BROKER_RECORD => handleFenceBrokerRecord(imageBuilders,
        record.asInstanceOf[FenceBrokerRecord])
      case UNFENCE_BROKER_RECORD => handleUnfenceBrokerRecord(imageBuilders,
        record.asInstanceOf[UnfenceBrokerRecord])
      case REMOVE_TOPIC_RECORD => handleRemoveTopicRecord(imageBuilders,
        record.asInstanceOf[RemoveTopicRecord])
      // TODO: handle FEATURE_LEVEL_RECORD
      case _ => throw new RuntimeException(s"Unsupported record type ${recordType}")
    }
  }

  def handleRegisterBrokerRecord(imageBuilders: Seq[Option[MetadataImageBuilder]],
                                 record: RegisterBrokerRecord): Unit = {
    val broker = MetadataBroker(record)
    imageBuilders.foreach(_.foreach(_.brokersBuilder().add(broker)))
  }

  def handleUnregisterBrokerRecord(imageBuilders: Seq[Option[MetadataImageBuilder]],
                                   record: UnregisterBrokerRecord): Unit = {
    imageBuilders.foreach(_.foreach(_.brokersBuilder().remove(record.brokerId())))
  }

  def handleTopicRecord(imageBuilders: Seq[Option[MetadataImageBuilder]],
                        record: TopicRecord): Unit = {
    imageBuilders.foreach(_.foreach(_.partitionsBuilder().addUuidMapping(record.name(), record.topicId())))
  }

  def handlePartitionRecord(imageBuilders: Seq[Option[MetadataImageBuilder]],
                            record: PartitionRecord): Unit = {
    imageBuilders.foreach(_.foreach(imageBuilder => {
      imageBuilder.topicIdToName(record.topicId()) match {
      case None => throw new RuntimeException(s"Unable to locate topic with ID ${record.topicId}")
      case Some(name) =>
        val partition = MetadataPartition(name, record)
        imageBuilder.partitionsBuilder().set(partition)
    }}))
  }

  def handleConfigRecord(record: ConfigRecord): Unit = {
    val t = ConfigResource.Type.forId(record.resourceType())
    if (t == ConfigResource.Type.UNKNOWN) {
      throw new RuntimeException("Unable to understand config resource type " +
        s"${Integer.valueOf(record.resourceType())}")
    }
    val resource = new ConfigResource(t, record.resourceName())
    configRepository.setConfig(resource, record.name(), record.value())
  }

  def handleIsrChangeRecord(imageBuilders: Seq[Option[MetadataImageBuilder]],
                            record: IsrChangeRecord): Unit = {
    imageBuilders.foreach(_.foreach(_.partitionsBuilder().handleIsrChange(record)))
  }

  def handleFenceBrokerRecord(imageBuilders: Seq[Option[MetadataImageBuilder]],
                              record: FenceBrokerRecord): Unit = {
    // TODO: add broker epoch to metadata cache, and check it here.
    imageBuilders.foreach(_.foreach(_.brokersBuilder().changeFencing(record.id(), true)))
  }

  def handleUnfenceBrokerRecord(imageBuilders: Seq[Option[MetadataImageBuilder]],
                                record: UnfenceBrokerRecord): Unit = {
    // TODO: add broker epoch to metadata cache, and check it here.
    imageBuilders.foreach(_.foreach(_.brokersBuilder().changeFencing(record.id(), false)))
  }

  def handleRemoveTopicRecord(imageBuilders: Seq[Option[MetadataImageBuilder]],
                              record: RemoveTopicRecord): Unit = {
    var removedPartitions: Iterable[MetadataPartition] = List.empty
    imageBuilders.foreach(_.foreach(imageBuilder => {
      removedPartitions = imageBuilder.partitionsBuilder().
        removeTopicById(record.topicId())
    }))
    if (preRecoveryDeletedPartitions.isDefined) {
      preRecoveryDeletedPartitions.foreach(_.addAll(removedPartitions))
    } else {
      groupCoordinator.handleDeletedPartitions(removedPartitions.map(_.toTopicPartition()).toSeq)
    }
  }

  class HandleNewLeaderEvent(leader: MetaLogLeader)
      extends EventQueue.FailureLoggingEvent(logger.underlying) {
    override def run(): Unit = {
      val imageBuilder =
        MetadataImageBuilder(brokerId, logger.underlying, metadataCache.currentImage())
      Seq(Option(imageBuilder), preRecoveryPartitionImageBuilder).foreach(_.foreach(imageBuilder =>
        if (leader.nodeId() < 0) {
          imageBuilder.setControllerId(None)
        } else {
          imageBuilder.setControllerId(Some(leader.nodeId()))
        }))
      metadataCache.setImage(imageBuilder.build())
    }
  }

  override def handleNewLeader(leader: MetaLogLeader): Unit = {
    eventQueue.append(new HandleNewLeaderEvent(leader))
  }

  class ShutdownEvent() extends EventQueue.FailureLoggingEvent(logger.underlying) {
    override def run(): Unit = {
      removeMetric(BrokerMetadataListener.MetadataBatchProcessingTimeUs)
      removeMetric(BrokerMetadataListener.MetadataBatchSizes)
    }
  }

  override def beginShutdown(): Unit = {
    eventQueue.beginShutdown("beginShutdown", new ShutdownEvent())
  }

  def close(): Unit = {
    beginShutdown()
    eventQueue.close()
  }
}
