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

import java.io.File
import java.util.Optional
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.locks.Lock

import kafka.api._
import kafka.cluster.{BrokerEndPoint, Partition}
import kafka.common.RecordValidationException
import kafka.controller.{KafkaController, StateChangeLogger}
import kafka.log._
import kafka.metrics.KafkaMetricsGroup
import kafka.server.{FetchMetadata => SFetchMetadata}
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.checkpoints.{LazyOffsetCheckpoints, OffsetCheckpointFile, OffsetCheckpoints}
import kafka.server.metadata.{ConfigRepository, MetadataBrokers, MetadataImageBuilder, MetadataPartition}
import kafka.utils._
import kafka.utils.Implicits._
import org.apache.kafka.common.{ElectionType, IsolationLevel, Node, TopicPartition}
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResult
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopic
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.{EpochEndOffset, OffsetForLeaderTopicResult}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record._
import org.apache.kafka.common.replica.PartitionView.DefaultPartitionView
import org.apache.kafka.common.replica.ReplicaView.DefaultReplicaView
import org.apache.kafka.common.replica.{ClientMetadata, _}
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.Time

import scala.jdk.CollectionConverters._
import scala.collection.{Map, Seq, Set, mutable}
import scala.compat.java8.OptionConverters._

/**
 * Trait to represent the state of hosted partitions. We create a concrete (active) Partition
 * instance when the broker receives a message on the metadata log indicating
 * that it should be either a leader or follower of a partition.
 */
sealed trait HostedPartitionRaft
object HostedPartitionRaft {
  /**
   * This broker does not have any state for this partition locally.
   */
  final object None extends HostedPartitionRaft

  /**
   * This broker hosts the partition and it is online.
   */
  final case class Online(partition: Partition) extends HostedPartitionRaft

  /**
   * This broker hosted the partition but it is deferring changes; status is unknown
   * (this only applies to brokers that are using a Raft-based metadata
   * quorum; it never happens when using ZooKeeper).
   */
  final case class Deferred(partition: Partition,
                            metadata: MetadataPartition,
                            wasNew: Boolean,
                            mostRecentMetadataOffset: Long,
                            onLeadershipChange: (Iterable[Partition], Iterable[Partition]) => Unit) extends HostedPartitionRaft

  /**
   * This broker hosts the partition, but it is in an offline log directory.
   */
  final object Offline extends HostedPartitionRaft
}

class ReplicaManagerRaft(val config: KafkaConfig,
                         metrics: Metrics,
                         time: Time,
                         scheduler: Scheduler,
                         val logManager: LogManager,
                         val isShuttingDown: AtomicBoolean,
                         quotaManagers: QuotaManagers,
                         val brokerTopicStats: BrokerTopicStats,
                         val metadataCache: MetadataCache,
                         logDirFailureChannel: LogDirFailureChannel,
                         val delayedProducePurgatory: DelayedOperationPurgatory[DelayedProduce],
                         val delayedFetchPurgatory: DelayedOperationPurgatory[DelayedFetch],
                         val delayedDeleteRecordsPurgatory: DelayedOperationPurgatory[DelayedDeleteRecords],
                         val delayedElectLeaderPurgatory: DelayedOperationPurgatory[DelayedElectLeader],
                         threadNamePrefix: Option[String],
                         val configRepository: ConfigRepository,
                         val alterIsrManager: AlterIsrManager,
                         val helper: ReplicaManagerHelper)
  extends ReplicaManager with Logging with KafkaMetricsGroup {

  def this(config: KafkaConfig,
           metrics: Metrics,
           time: Time,
           scheduler: Scheduler,
           logManager: LogManager,
           isShuttingDown: AtomicBoolean,
           quotaManagers: QuotaManagers,
           brokerTopicStats: BrokerTopicStats,
           metadataCache: MetadataCache,
           logDirFailureChannel: LogDirFailureChannel,
           alterIsrManager: AlterIsrManager,
           configRepository: ConfigRepository,
           threadNamePrefix: Option[String]) = {
    this(config, metrics, time, scheduler, logManager, isShuttingDown,
      quotaManagers, brokerTopicStats, metadataCache, logDirFailureChannel,
      DelayedOperationPurgatory[DelayedProduce](
        purgatoryName = "Produce", brokerId = config.brokerId,
        purgeInterval = config.producerPurgatoryPurgeIntervalRequests),
      DelayedOperationPurgatory[DelayedFetch](
        purgatoryName = "Fetch", brokerId = config.brokerId,
        purgeInterval = config.fetchPurgatoryPurgeIntervalRequests),
      DelayedOperationPurgatory[DelayedDeleteRecords](
        purgatoryName = "DeleteRecords", brokerId = config.brokerId,
        purgeInterval = config.deleteRecordsPurgatoryPurgeIntervalRequests),
      DelayedOperationPurgatory[DelayedElectLeader](
        purgatoryName = "ElectLeader", brokerId = config.brokerId),
      threadNamePrefix, configRepository, alterIsrManager,
      new ReplicaManagerHelper())
  }
  val zkClient: Option[kafka.zk.KafkaZkClient] = None

  /* epoch of the controller that last changed the leader */
  @volatile var controllerEpoch: Int = KafkaController.InitialControllerEpoch
  private val localBrokerId = config.brokerId
  private val allPartitions = new Pool[TopicPartition, HostedPartitionRaft](
    valueFactory = Some(tp => HostedPartitionRaft.Online(Partition(tp, time, this)))
  )
  override def allPartitionsCount: Int = allPartitions.size
  private val replicaStateChangeLock = new Object
  val replicaFetcherManager = createReplicaFetcherManager(metrics, time, threadNamePrefix, quotaManagers.follower)
  def getReplicaFetcherManager(): ReplicaFetcherManager = replicaFetcherManager
  val replicaAlterLogDirsManager = createReplicaAlterLogDirsManager(quotaManagers.alterLogDirs, brokerTopicStats)
  private val highWatermarkCheckPointThreadStarted = new AtomicBoolean(false)
  @volatile var highWatermarkCheckpoints: Map[String, OffsetCheckpointFile] = logManager.liveLogDirs.map(dir =>
    (dir.getAbsolutePath, new OffsetCheckpointFile(new File(dir, ReplicaManager.HighWatermarkFilename), logDirFailureChannel))).toMap

  this.logIdent = s"[ReplicaManagerRaft broker=$localBrokerId] "
  private val stateChangeLogger = new StateChangeLogger(localBrokerId, inControllerContext = false, None)

  private val isrChangeNotificationConfig = ReplicaManager.DefaultIsrPropagationConfig
  private val isrChangeSet: mutable.Set[TopicPartition] = new mutable.HashSet[TopicPartition]()
  private val lastIsrChangeMs = new AtomicLong(time.milliseconds())
  private val lastIsrPropagationMs = new AtomicLong(time.milliseconds())

  private var logDirFailureHandler: LogDirFailureHandler = null

  // Changes are initially deferrable when using a Raft-based metadata quorum, and may flip-flop thereafter;
  // changes are never deferrable when using ZooKeeper.  When true, this indicates that we should transition online
  // partitions to the deferred state if we see metadata with a different leader epoch.
  @volatile private var changesDeferrable: Boolean = true
  stateChangeLogger.info(s"Metadata changes deferrable=$changesDeferrable")

  def deferrableMetadataChanges(): Unit = {
    replicaStateChangeLock synchronized {
      changesDeferrable = true
      stateChangeLogger.info(s"Metadata changes are now deferrable")
    }
  }

  private def applyDeferredMetadataChanges(): Unit = {
    val startMs = time.milliseconds()
    replicaStateChangeLock synchronized {
      stateChangeLogger.info(s"Applying deferred metadata changes")
      val highWatermarkCheckpoints = new LazyOffsetCheckpoints(this.highWatermarkCheckpoints)
      val partitionsMadeFollower = mutable.Set[Partition]()
      val partitionsMadeLeader = mutable.Set[Partition]()
      val leadershipChangeCallbacks =
        mutable.Map[(Iterable[Partition], Iterable[Partition]) => Unit, (mutable.Set[Partition], mutable.Set[Partition])]()
      try {
        val leaderPartitionStates = mutable.Map[Partition, MetadataPartition]()
        val followerPartitionStates = mutable.Map[Partition, MetadataPartition]()
        val partitionsAlreadyExisting = mutable.Set[MetadataPartition]()
        val mostRecentMetadataOffsets = mutable.Map[Partition, Long]()
        deferredPartitionsIterator.foreach { deferredPartition =>
          val state = deferredPartition.metadata
          val partition = deferredPartition.partition
          if (state.leaderId == localBrokerId) {
            leaderPartitionStates.put(partition, state)
          } else {
            followerPartitionStates.put(partition, state)
          }
          if (!deferredPartition.wasNew) {
            partitionsAlreadyExisting += state
          }
          mostRecentMetadataOffsets.put(partition, deferredPartition.mostRecentMetadataOffset)
        }

        val partitionsMadeLeader = makeLeaders(partitionsAlreadyExisting, leaderPartitionStates,
          highWatermarkCheckpoints,-1, mostRecentMetadataOffsets)
        val partitionsMadeFollower = makeFollowers(partitionsAlreadyExisting,
          metadataCache.currentImage().brokers, followerPartitionStates,
          highWatermarkCheckpoints, -1, mostRecentMetadataOffsets)

        // We need to transition anything that hasn't transitioned from Deferred to Offline to the Online state.
        // We also need to identify the leadership change callback(s) to invoke
        deferredPartitionsIterator.foreach { deferredPartition =>
          val state = deferredPartition.metadata
          val partition = deferredPartition.partition
          val topicPartition = partition.topicPartition
          // identify for callback if necessary
          if (state.leaderId == localBrokerId) {
            if (partitionsMadeLeader.contains(partition)) {
              leadershipChangeCallbacks.getOrElseUpdate(
                deferredPartition.onLeadershipChange, (mutable.Set(), mutable.Set()))._1 += partition
            }
          } else if (partitionsMadeFollower.contains(partition)) {
            leadershipChangeCallbacks.getOrElseUpdate(
              deferredPartition.onLeadershipChange, (mutable.Set(), mutable.Set()))._2 += partition
          }
          // transition from Deferred to Online
          allPartitions.put(topicPartition, HostedPartitionRaft.Online(partition))
        }

        helper.updateLeaderAndFollowerMetrics(this, partitionsMadeFollower.map(_.topic).toSet)

        helper.maybeAddLogDirFetchers(this, partitionsMadeFollower, highWatermarkCheckpoints)

        replicaFetcherManager.shutdownIdleFetcherThreads()
        replicaAlterLogDirsManager.shutdownIdleFetcherThreads()
        leadershipChangeCallbacks.forKeyValue { (onLeadershipChange, leaderAndFollowerPartitions) =>
          onLeadershipChange(leaderAndFollowerPartitions._1, leaderAndFollowerPartitions._2)
        }
      } catch {
        case e: Throwable =>
          deferredPartitionsIterator.foreach { metadata =>
            val state = metadata.metadata
            val partition = metadata.partition
            val topicPartition = partition.topicPartition
            val mostRecentMetadataOffset = metadata.mostRecentMetadataOffset
            val leader = state.leaderId == localBrokerId
            val leaderOrFollower = if (leader) "leader" else "follower"
            val partitionLogMsgPrefix = s"Apply deferred $leaderOrFollower partition $topicPartition last seen in metadata batch $mostRecentMetadataOffset"
            stateChangeLogger.error(s"$partitionLogMsgPrefix: error while applying deferred metadata.", e)
          }
          stateChangeLogger.info(s"Applied ${partitionsMadeLeader.size + partitionsMadeFollower.size} deferred partitions prior to the error: " +
            s"${partitionsMadeLeader.size} leader(s) and ${partitionsMadeFollower.size} follower(s)")
          // Re-throw the exception for it to be caught in BrokerMetadataListener
          throw e
      }
      changesDeferrable = false
      val endMs = time.milliseconds()
      val elapsedMs = endMs - startMs
      stateChangeLogger.info(s"Applied ${partitionsMadeLeader.size + partitionsMadeFollower.size} deferred partitions: " +
        s"${partitionsMadeLeader.size} leader(s) and ${partitionsMadeFollower.size} follower(s)" +
        s"in $elapsedMs ms")
      stateChangeLogger.info("Metadata changes are not deferrable")
    }
  }

  private class LogDirFailureHandler(name: String, haltBrokerOnDirFailure: Boolean) extends ShutdownableThread(name) {
    override def doWork(): Unit = {
      val newOfflineLogDir = logDirFailureChannel.takeNextOfflineLogDir()
      if (haltBrokerOnDirFailure) {
        fatal(s"Halting broker because dir $newOfflineLogDir is offline")
        Exit.halt(1)
      }
      handleLogDirFailure(newOfflineLogDir)
    }
  }

  // Visible for testing
  private[server] val replicaSelectorOpt: Option[ReplicaSelector] = createReplicaSelector()

  def startHighWatermarkCheckPointThread(): Unit = {
    if (highWatermarkCheckPointThreadStarted.compareAndSet(false, true))
      scheduler.schedule("highwatermark-checkpoint", checkpointHighWatermarks _, period = config.replicaHighWatermarkCheckpointIntervalMs, unit = TimeUnit.MILLISECONDS)
  }

  def recordIsrChange(topicPartition: TopicPartition): Unit = {
    if (!config.interBrokerProtocolVersion.isAlterIsrSupported) {
      isrChangeSet synchronized {
        isrChangeSet += topicPartition
        lastIsrChangeMs.set(time.milliseconds())
      }
    }
  }
  /**
   * This function periodically runs to see if ISR needs to be propagated. It propagates ISR when:
   * 1. There is ISR change not propagated yet.
   * 2. There is no ISR Change in the last five seconds, or it has been more than 60 seconds since the last ISR propagation.
   * This allows an occasional ISR change to be propagated within a few seconds, and avoids overwhelming controller and
   * other brokers when large amount of ISR change occurs.
   */
  def maybePropagateIsrChanges(): Unit = {
    val now = time.milliseconds()
    isrChangeSet synchronized {
      if (isrChangeSet.nonEmpty &&
        (lastIsrChangeMs.get() + isrChangeNotificationConfig.lingerMs < now ||
          lastIsrPropagationMs.get() + isrChangeNotificationConfig.maxDelayMs < now)) {
        zkClient.foreach(_.propagateIsrChanges(isrChangeSet))
        isrChangeSet.clear()
        lastIsrPropagationMs.set(now)
      }
    }
  }

  // When ReplicaAlterDirThread finishes replacing a current replica with a future replica, it will
  // remove the partition from the partition state map. But it will not close itself even if the
  // partition state map is empty. Thus we need to call shutdownIdleReplicaAlterDirThread() periodically
  // to shutdown idle ReplicaAlterDirThread
  def shutdownIdleReplicaAlterLogDirsThread(): Unit = {
    replicaAlterLogDirsManager.shutdownIdleFetcherThreads()
  }

  def getLog(topicPartition: TopicPartition): Option[Log] = logManager.getLog(topicPartition)

  def startup(): Unit = {
    // start ISR expiration thread
    // A follower can lag behind leader for up to config.replicaLagTimeMaxMs x 1.5 before it is removed from ISR
    scheduler.schedule("isr-expiration", maybeShrinkIsr _, period = config.replicaLagTimeMaxMs / 2, unit = TimeUnit.MILLISECONDS)
    // If using AlterIsr, we don't need the znode ISR propagation
    if (!config.interBrokerProtocolVersion.isAlterIsrSupported) {
      throw new IllegalStateException(s"Must use at least IBP KAFKA_2_7_IV2 with Raft-based quorums: ${config.interBrokerProtocolVersion}")
    } else {
      alterIsrManager.start()
    }
    scheduler.schedule("shutdown-idle-replica-alter-log-dirs-thread", shutdownIdleReplicaAlterLogDirsThread _, period = 10000L, unit = TimeUnit.MILLISECONDS)

    // If inter-broker protocol (IBP) < 1.0, the controller will send LeaderAndIsrRequest V0 which does not include isNew field.
    // In this case, the broker receiving the request cannot determine whether it is safe to create a partition if a log directory has failed.
    // Thus, we choose to halt the broker on any log diretory failure if IBP < 1.0
    val haltBrokerOnFailure = config.interBrokerProtocolVersion < KAFKA_1_0_IV0
    logDirFailureHandler = new LogDirFailureHandler("LogDirFailureHandler", haltBrokerOnFailure)
    logDirFailureHandler.start()
    applyDeferredMetadataChanges()
  }

  private def maybeRemoveTopicMetrics(topic: String): Unit = {
    val topicHasOnlineOrDeferredPartition = allPartitions.values.exists {
      case HostedPartitionRaft.Online(partition) => topic == partition.topic
      case HostedPartitionRaft.Deferred(partition, _, _, _, _) => topic == partition.topic
      case HostedPartitionRaft.None | HostedPartitionRaft.Offline => false
    }
    if (!topicHasOnlineOrDeferredPartition)
      brokerTopicStats.removeMetrics(topic)
  }

  /**
   * Stop the given partitions.
   *
   * @param partitionsToStop    A map from a topic partition to a boolean indicating
   *                            whether the partition should be deleted.
   *
   * @return                    A map from partitions to exceptions which occurred.
   *                            If no errors occurred, the map will be empty.
   */
  private def stopPartitions(partitionsToStop: Map[TopicPartition, Boolean]):
      Map[TopicPartition, Throwable] = {
    // First stop fetchers for all partitions.
    val partitions = partitionsToStop.keySet
    replicaFetcherManager.removeFetcherForPartitions(partitions)
    replicaAlterLogDirsManager.removeFetcherForPartitions(partitions)

    // Second remove deleted partitions from the partition map. Fetchers rely on the
    // ReplicaManager to get Partition's information so they must be stopped first.

    def deletePartition(topicPartition: TopicPartition, hostedPartition: HostedPartitionRaft, partition: Partition) = {
      if (allPartitions.remove(topicPartition, hostedPartition)) {
        maybeRemoveTopicMetrics(topicPartition.topic)
        // Logs are not deleted here. They are deleted in a single batch later on.
        // This is done to avoid having to checkpoint for every deletions.
        partition.delete()
      }
    }

    val partitionsToDelete = mutable.Set.empty[TopicPartition]
    partitionsToStop.forKeyValue { (topicPartition, shouldDelete) =>
      if (shouldDelete) {
        getPartition(topicPartition) match {
          case hostedPartition@HostedPartitionRaft.Online(partition) =>
            deletePartition(topicPartition, hostedPartition, partition)
          case hostedPartition@HostedPartitionRaft.Deferred(partition, _, _, _, _) =>
            deletePartition(topicPartition, hostedPartition, partition)
          case _ =>
        }
        partitionsToDelete += topicPartition
      }
      // If we were the leader, we may have some operations still waiting for completion.
      // We force completion to prevent them from timing out.
      helper.completeDelayedFetchOrProduceRequests(this, topicPartition)
    }

    val errorMap = new mutable.HashMap[TopicPartition, Throwable]()
    if (!partitionsToDelete.isEmpty) {
      // Delete the logs and checkpoint. Confusingly, this function isn't actually
      // asynchronous-- it just synchronously schedules the directories to be deleted later.
      logManager.asyncDelete(partitionsToDelete, (tp, e) => errorMap.put(tp, e))
    }
    errorMap
  }

  private def getPartition(topicPartition: TopicPartition): HostedPartitionRaft = {
    Option(allPartitions.get(topicPartition)).getOrElse(HostedPartitionRaft.None)
  }

  // This only considers online partitions; in particular, it does not consider deferred partitions
  override def isAddingReplica(topicPartition: TopicPartition, replicaId: Int): Boolean = {
    getPartition(topicPartition) match {
      case HostedPartitionRaft.Online(partition) => partition.isAddingReplica(replicaId)
      case _ => false
    }
  }

  // Creates an online partition; visible for testing
  def createPartition(topicPartition: TopicPartition): Partition = {
    val partition = Partition(topicPartition, time, this)
    allPartitions.put(topicPartition, HostedPartitionRaft.Online(partition))
    partition
  }

  // Creates an deferred partition; visible for testing
  private[server] def createDeferredPartition(topicPartition: TopicPartition,
                                              metadata: MetadataPartition,
                                              wasNew: Boolean,
                                              mostRecentMetadataOffset: Long,
                                              onLeadershipChange: (Iterable[Partition], Iterable[Partition]) => Unit): Partition = {
    val partition = Partition(topicPartition, time, this)
    allPartitions.put(topicPartition,
      HostedPartitionRaft.Deferred(
        partition,
        metadata,
        wasNew,
        mostRecentMetadataOffset,
        onLeadershipChange))
    partition
  }

  // Returns online partitions only.
  // Equivalent to deferredOrOnlinePartition() when using ZooKeeper
  def onlinePartition(topicPartition: TopicPartition): Option[Partition] = {
    getPartition(topicPartition) match {
      case HostedPartitionRaft.Online(partition) => Some(partition)
      case _ => None
    }
  }

  // Returns online as well as deferred partitions.
  // Equivalent to onlinePartition() when using ZooKeeper
  private def deferredOrOnlinePartition(topicPartition: TopicPartition): Option[Partition] = {
    getPartition(topicPartition) match {
      case HostedPartitionRaft.Online(partition) => Some(partition)
      case HostedPartitionRaft.Deferred(partition, _, _, _, _) => Some(partition)
      case HostedPartitionRaft.None | HostedPartitionRaft.Offline => None
    }
  }

  private def isDeferred(topicPartition: TopicPartition): Boolean = {
    getPartition(topicPartition) match {
      case HostedPartitionRaft.Deferred(_, _, _, _, _) => true
      case _ => false
    }
  }

  // An iterator over all online partition only. This is a weakly consistent iterator; a partition made offline or deferred
  // after the iterator has been constructed could still be returned by this iterator.
  // Equivalent to deferredOrOnlinePartitionsIterator() when using ZooKeeper.
  def onlinePartitionsIterator: Iterator[Partition] = {
    allPartitions.values.iterator.flatMap {
      case HostedPartitionRaft.Online(partition) => Some(partition)
      case _ => None
    }
  }

  // An iterator over all deferred or online partitions. This is a weakly consistent iterator; a partition made offline after
  // the iterator has been constructed could still be returned by this iterator.
  // Equivalent to onlinePartitionsIterator() when using ZooKeeper.
  private def deferredOrOnlinePartitionsIterator: Iterator[Partition] = {
    allPartitions.values.iterator.flatMap {
      case HostedPartitionRaft.Online(partition) => Some(partition)
      case HostedPartitionRaft.Deferred(partition, _, _, _, _) => Some(partition)
      case HostedPartitionRaft.None | HostedPartitionRaft.Offline => None
    }
  }

  def offlinePartitionCount: Int = {
    allPartitions.values.iterator.count(_ == HostedPartitionRaft.Offline)
  }

  // An iterator over all deferred partitions. This is a weakly consistent iterator; a partition made off/online
  // after the iterator has been constructed could still be returned by this iterator.
  private def deferredPartitionsIterator: Iterator[HostedPartitionRaft.Deferred] = {
    allPartitions.values.iterator.flatMap {
      case deferred@HostedPartitionRaft.Deferred(_, _, _, _, _) => Some(deferred)
      case _ => None
    }
  }

  // Return the partition if it is online, otherwise raises an exception.
  // Equivalent to deferredOrOnlinePartitionOrException() when using ZooKeeper.
  def onlinePartitionOrException(topicPartition: TopicPartition): Partition = {
    onlinePartitionOrError(topicPartition) match {
      case Left(Errors.KAFKA_STORAGE_ERROR) =>
        throw new KafkaStorageException(s"Partition $topicPartition is in an offline log directory")

      case Left(error) =>
        throw error.exception(s"Error while fetching partition state for $topicPartition")

      case Right(partition) => partition
    }
  }

  // Return the partition if it is online, otherwise an error.
  // Equivalent to deferredOrOnlinePartitionOrError() when using ZooKeeper.
  def onlinePartitionOrError(topicPartition: TopicPartition): Either[Errors, Partition] = {
    getPartition(topicPartition) match {
      case HostedPartitionRaft.Online(partition) =>
        Right(partition)

      case HostedPartitionRaft.Deferred(_, _, _, _, _) =>
        Left(Errors.NOT_LEADER_OR_FOLLOWER)

      case HostedPartitionRaft.Offline =>
        Left(Errors.KAFKA_STORAGE_ERROR)

      case HostedPartitionRaft.None if metadataCache.contains(topicPartition) =>
        // The topic exists, but this broker is no longer a replica of it, so we return NOT_LEADER_OR_FOLLOWER which
        // forces clients to refresh metadata to find the new location. This can happen, for example,
        // during a partition reassignment if a produce request from the client is sent to a broker after
        // the local replica has been deleted.
        Left(Errors.NOT_LEADER_OR_FOLLOWER)

      case HostedPartitionRaft.None =>
        Left(Errors.UNKNOWN_TOPIC_OR_PARTITION)
    }
  }

  // Returns the Log for the partition if it is deferred or online.
  // Equivalent to localOnlineLog() when using ZooKeeper.
  private def localDeferredOrOnlineLog(topicPartition: TopicPartition): Option[Log] = {
    deferredOrOnlinePartition(topicPartition).flatMap(_.log)
  }

  def getLogDir(topicPartition: TopicPartition): Option[String] = {
    localDeferredOrOnlineLog(topicPartition).map(_.parentDir)
  }

  /**
   * TODO: move this action queue to handle thread so we can simplify concurrency handling
   */
  private val actionQueue = new ActionQueue

  def tryCompleteActions(): Unit = actionQueue.tryCompleteActions()

  /**
   * Append messages to leader replicas of the partition, and wait for them to be replicated to other replicas;
   * the callback function will be triggered either when timeout or the required acks are satisfied;
   * if the callback function itself is already synchronized on some object then pass this object to avoid deadlock.
   *
   * Noted that all pending delayed check operations are stored in a queue. All callers to ReplicaManager.appendRecords()
   * are expected to call ActionQueue.tryCompleteActions for all affected partitions, without holding any conflicting
   * locks.
   */
  def appendRecords(timeout: Long,
                    requiredAcks: Short,
                    internalTopicsAllowed: Boolean,
                    origin: AppendOrigin,
                    entriesPerPartition: Map[TopicPartition, MemoryRecords],
                    responseCallback: Map[TopicPartition, PartitionResponse] => Unit,
                    delayedProduceLock: Option[Lock] = None,
                    recordConversionStatsCallback: Map[TopicPartition, RecordConversionStats] => Unit = _ => ()): Unit = {
    if (isValidRequiredAcks(requiredAcks)) {
      val sTime = time.milliseconds
      val localProduceResults = appendToLocalLog(internalTopicsAllowed = internalTopicsAllowed,
        origin, entriesPerPartition, requiredAcks)
      debug("Produce to local log in %d ms".format(time.milliseconds - sTime))

      val produceStatus = localProduceResults.map { case (topicPartition, result) =>
        topicPartition ->
                ProducePartitionStatus(
                  result.info.lastOffset + 1, // required offset
                  new PartitionResponse(result.error, result.info.firstOffset.getOrElse(-1), result.info.logAppendTime,
                    result.info.logStartOffset, result.info.recordErrors.asJava, result.info.errorMessage)) // response status
      }

      actionQueue.add {
        () =>
          localProduceResults.foreach {
            case (topicPartition, result) =>
              val requestKey = TopicPartitionOperationKey(topicPartition)
              result.info.leaderHwChange match {
                case LeaderHwChange.Increased =>
                  // some delayed operations may be unblocked after HW changed
                  delayedProducePurgatory.checkAndComplete(requestKey)
                  delayedFetchPurgatory.checkAndComplete(requestKey)
                  delayedDeleteRecordsPurgatory.checkAndComplete(requestKey)
                case LeaderHwChange.Same =>
                  // probably unblock some follower fetch requests since log end offset has been updated
                  delayedFetchPurgatory.checkAndComplete(requestKey)
                case LeaderHwChange.None =>
                  // nothing
              }
          }
      }

      recordConversionStatsCallback(localProduceResults.map { case (k, v) => k -> v.info.recordConversionStats })

      if (delayedProduceRequestRequired(requiredAcks, entriesPerPartition, localProduceResults)) {
        // create delayed produce operation
        val produceMetadata = ProduceMetadata(requiredAcks, produceStatus)
        val delayedProduce = new DelayedProduce(timeout, produceMetadata, this, responseCallback, delayedProduceLock)

        // create a list of (topic, partition) pairs to use as keys for this delayed produce operation
        val producerRequestKeys = entriesPerPartition.keys.map(TopicPartitionOperationKey(_)).toSeq

        // try to complete the request immediately, otherwise put it into the purgatory
        // this is because while the delayed produce operation is being created, new
        // requests may arrive and hence make this operation completable.
        delayedProducePurgatory.tryCompleteElseWatch(delayedProduce, producerRequestKeys)

      } else {
        // we can respond immediately
        val produceResponseStatus = produceStatus.map { case (k, status) => k -> status.responseStatus }
        responseCallback(produceResponseStatus)
      }
    } else {
      // If required.acks is outside accepted range, something is wrong with the client
      // Just return an error and don't handle the request at all
      val responseStatus = entriesPerPartition.map { case (topicPartition, _) =>
        topicPartition -> new PartitionResponse(Errors.INVALID_REQUIRED_ACKS,
          LogAppendInfo.UnknownLogAppendInfo.firstOffset.getOrElse(-1), RecordBatch.NO_TIMESTAMP, LogAppendInfo.UnknownLogAppendInfo.logStartOffset)
      }
      responseCallback(responseStatus)
    }
  }

  /**
   * Delete records on leader replicas of the partition, and wait for delete records operation be propagated to other replicas;
   * the callback function will be triggered either when timeout or logStartOffset of all live replicas have reached the specified offset
   */
  private def deleteRecordsOnLocalLog(offsetPerPartition: Map[TopicPartition, Long]): Map[TopicPartition, LogDeleteRecordsResult] = {
    trace("Delete records on local logs to offsets [%s]".format(offsetPerPartition))
    offsetPerPartition.map { case (topicPartition, requestedOffset) =>
      // reject delete records operation on internal topics
      if (Topic.isInternal(topicPartition.topic)) {
        (topicPartition, LogDeleteRecordsResult(-1L, -1L, Some(new InvalidTopicException(s"Cannot delete records of internal topic ${topicPartition.topic}"))))
      } else {
        try {
          val partition = onlinePartitionOrException(topicPartition)
          val logDeleteResult = partition.deleteRecordsOnLeader(requestedOffset)
          (topicPartition, logDeleteResult)
        } catch {
          case e@ (_: UnknownTopicOrPartitionException |
                   _: NotLeaderOrFollowerException |
                   _: OffsetOutOfRangeException |
                   _: PolicyViolationException |
                   _: KafkaStorageException) =>
            (topicPartition, LogDeleteRecordsResult(-1L, -1L, Some(e)))
          case t: Throwable =>
            error("Error processing delete records operation on partition %s".format(topicPartition), t)
            (topicPartition, LogDeleteRecordsResult(-1L, -1L, Some(t)))
        }
      }
    }
  }

  // If there exists a topic partition that meets the following requirement,
  // we need to put a delayed DeleteRecordsRequest and wait for the delete records operation to complete
  //
  // 1. the delete records operation on this partition is successful
  // 2. low watermark of this partition is smaller than the specified offset
  private def delayedDeleteRecordsRequired(localDeleteRecordsResults: Map[TopicPartition, LogDeleteRecordsResult]): Boolean = {
    localDeleteRecordsResults.exists{ case (_, deleteRecordsResult) =>
      deleteRecordsResult.exception.isEmpty && deleteRecordsResult.lowWatermark < deleteRecordsResult.requestedOffset
    }
  }

  /**
   * For each pair of partition and log directory specified in the map, if the partition has already been created on
   * this broker, move its log files to the specified log directory. Otherwise, record the pair in the memory so that
   * the partition will be created in the specified log directory when broker receives LeaderAndIsrRequest for the partition later.
   */
  def alterReplicaLogDirs(partitionDirs: Map[TopicPartition, String]): Map[TopicPartition, Errors] = {
    replicaStateChangeLock synchronized {
      partitionDirs.map { case (topicPartition, destinationDir) =>
        try {
          /* If the topic name is exceptionally long, we can't support altering the log directory.
           * See KAFKA-4893 for details.
           * TODO: fix this by implementing topic IDs. */
          if (Log.logFutureDirName(topicPartition).size > 255)
            throw new InvalidTopicException("The topic name is too long.")
          if (!logManager.isLogDirOnline(destinationDir))
            throw new KafkaStorageException(s"Log directory $destinationDir is offline")

          getPartition(topicPartition) match {
            case HostedPartitionRaft.Online(partition) =>
              // Stop current replica movement if the destinationDir is different from the existing destination log directory
              if (partition.futureReplicaDirChanged(destinationDir)) {
                replicaAlterLogDirsManager.removeFetcherForPartitions(Set(topicPartition))
                partition.removeFutureLocalReplica()
              }
            case HostedPartitionRaft.Deferred(_, _, _, _, _) =>
              throw new ReplicaNotAvailableException(s"Partition $topicPartition is deferred")
            case HostedPartitionRaft.Offline =>
              throw new KafkaStorageException(s"Partition $topicPartition is offline")

            case HostedPartitionRaft.None => // Do nothing
          }

          // If the log for this partition has not been created yet:
          // 1) Record the destination log directory in the memory so that the partition will be created in this log directory
          //    when broker receives LeaderAndIsrRequest for this partition later.
          // 2) Respond with NotLeaderOrFollowerException for this partition in the AlterReplicaLogDirsResponse
          logManager.maybeUpdatePreferredLogDir(topicPartition, destinationDir)

          // throw NotLeaderOrFollowerException if replica does not exist for the given partition
          val partition = onlinePartitionOrException(topicPartition)
          partition.localLogOrException

          // If the destinationLDir is different from the current log directory of the replica:
          // - If there is no offline log directory, create the future log in the destinationDir (if it does not exist) and
          //   start ReplicaAlterDirThread to move data of this partition from the current log to the future log
          // - Otherwise, return KafkaStorageException. We do not create the future log while there is offline log directory
          //   so that we can avoid creating future log for the same partition in multiple log directories.
          val highWatermarkCheckpoints = new LazyOffsetCheckpoints(this.highWatermarkCheckpoints)
          if (partition.maybeCreateFutureReplica(destinationDir, highWatermarkCheckpoints)) {
            val futureLog = futureLocalOnlineLogOrException(topicPartition)
            logManager.abortAndPauseCleaning(topicPartition)

            val initialFetchState = InitialFetchState(BrokerEndPoint(config.brokerId, "localhost", -1),
              partition.getLeaderEpoch, futureLog.highWatermark)
            replicaAlterLogDirsManager.addFetcherForPartitions(Map(topicPartition -> initialFetchState))
          }

          (topicPartition, Errors.NONE)
        } catch {
          case e@(_: InvalidTopicException |
                  _: LogDirNotFoundException |
                  _: ReplicaNotAvailableException |
                  _: KafkaStorageException) =>
            warn(s"Unable to alter log dirs for $topicPartition", e)
            (topicPartition, Errors.forException(e))
          case e: NotLeaderOrFollowerException =>
            // Retaining REPLICA_NOT_AVAILABLE exception for ALTER_REPLICA_LOG_DIRS for compatibility
            warn(s"Unable to alter log dirs for $topicPartition", e)
            (topicPartition, Errors.REPLICA_NOT_AVAILABLE)
          case t: Throwable =>
            error("Error while changing replica dir for partition %s".format(topicPartition), t)
            (topicPartition, Errors.forException(t))
        }
      }
    }
  }

  def deleteRecords(timeout: Long,
                    offsetPerPartition: Map[TopicPartition, Long],
                    responseCallback: Map[TopicPartition, DeleteRecordsPartitionResult] => Unit): Unit = {
    val timeBeforeLocalDeleteRecords = time.milliseconds
    val localDeleteRecordsResults = deleteRecordsOnLocalLog(offsetPerPartition)
    debug("Delete records on local log in %d ms".format(time.milliseconds - timeBeforeLocalDeleteRecords))

    val deleteRecordsStatus = localDeleteRecordsResults.map { case (topicPartition, result) =>
      topicPartition ->
        DeleteRecordsPartitionStatus(
          result.requestedOffset, // requested offset
          new DeleteRecordsPartitionResult()
            .setLowWatermark(result.lowWatermark)
            .setErrorCode(result.error.code)
            .setPartitionIndex(topicPartition.partition)) // response status
    }

    if (delayedDeleteRecordsRequired(localDeleteRecordsResults)) {
      // create delayed delete records operation
      val delayedDeleteRecords = new DelayedDeleteRecords(timeout, deleteRecordsStatus, this, responseCallback)

      // create a list of (topic, partition) pairs to use as keys for this delayed delete records operation
      val deleteRecordsRequestKeys = offsetPerPartition.keys.map(TopicPartitionOperationKey(_)).toSeq

      // try to complete the request immediately, otherwise put it into the purgatory
      // this is because while the delayed delete records operation is being created, new
      // requests may arrive and hence make this operation completable.
      delayedDeleteRecordsPurgatory.tryCompleteElseWatch(delayedDeleteRecords, deleteRecordsRequestKeys)
    } else {
      // we can respond immediately
      val deleteRecordsResponseStatus = deleteRecordsStatus.map { case (k, status) => k -> status.responseStatus }
      responseCallback(deleteRecordsResponseStatus)
    }
  }

  // If all the following conditions are true, we need to put a delayed produce request and wait for replication to complete
  //
  // 1. required acks = -1
  // 2. there is data to append
  // 3. at least one partition append was successful (fewer errors than partitions)
  private def delayedProduceRequestRequired(requiredAcks: Short,
                                            entriesPerPartition: Map[TopicPartition, MemoryRecords],
                                            localProduceResults: Map[TopicPartition, LogAppendResult]): Boolean = {
    requiredAcks == -1 &&
    entriesPerPartition.nonEmpty &&
    localProduceResults.values.count(_.exception.isDefined) < entriesPerPartition.size
  }

  private def isValidRequiredAcks(requiredAcks: Short): Boolean = {
    requiredAcks == -1 || requiredAcks == 1 || requiredAcks == 0
  }

  /**
   * Append the messages to the local replica logs
   */
  private def appendToLocalLog(internalTopicsAllowed: Boolean,
                               origin: AppendOrigin,
                               entriesPerPartition: Map[TopicPartition, MemoryRecords],
                               requiredAcks: Short): Map[TopicPartition, LogAppendResult] = {
    val traceEnabled = isTraceEnabled
    def processFailedRecord(topicPartition: TopicPartition, t: Throwable) = {
      val logStartOffset = getPartition(topicPartition) match {
        case HostedPartitionRaft.Online(partition) => partition.logStartOffset
        case HostedPartitionRaft.Deferred(_, _, _, _, _) => -1L
        case HostedPartitionRaft.None | HostedPartitionRaft.Offline => -1L
      }
      brokerTopicStats.topicStats(topicPartition.topic).failedProduceRequestRate.mark()
      brokerTopicStats.allTopicsStats.failedProduceRequestRate.mark()
      error(s"Error processing append operation on partition $topicPartition", t)

      logStartOffset
    }

    if (traceEnabled)
      trace(s"Append [$entriesPerPartition] to local log")

    entriesPerPartition.map { case (topicPartition, records) =>
      brokerTopicStats.topicStats(topicPartition.topic).totalProduceRequestRate.mark()
      brokerTopicStats.allTopicsStats.totalProduceRequestRate.mark()

      // reject appending to internal topics if it is not allowed
      if (Topic.isInternal(topicPartition.topic) && !internalTopicsAllowed) {
        (topicPartition, LogAppendResult(
          LogAppendInfo.UnknownLogAppendInfo,
          Some(new InvalidTopicException(s"Cannot append to internal topic ${topicPartition.topic}"))))
      } else {
        try {
          val partition = onlinePartitionOrException(topicPartition)
          val info = partition.appendRecordsToLeader(records, origin, requiredAcks)
          val numAppendedMessages = info.numMessages

          // update stats for successfully appended bytes and messages as bytesInRate and messageInRate
          brokerTopicStats.topicStats(topicPartition.topic).bytesInRate.mark(records.sizeInBytes)
          brokerTopicStats.allTopicsStats.bytesInRate.mark(records.sizeInBytes)
          brokerTopicStats.topicStats(topicPartition.topic).messagesInRate.mark(numAppendedMessages)
          brokerTopicStats.allTopicsStats.messagesInRate.mark(numAppendedMessages)

          if (traceEnabled)
            trace(s"${records.sizeInBytes} written to log $topicPartition beginning at offset " +
              s"${info.firstOffset.getOrElse(-1)} and ending at offset ${info.lastOffset}")

          (topicPartition, LogAppendResult(info))
        } catch {
          // NOTE: Failed produce requests metric is not incremented for known exceptions
          // it is supposed to indicate un-expected failures of a broker in handling a produce request
          case e@ (_: UnknownTopicOrPartitionException |
                   _: NotLeaderOrFollowerException |
                   _: RecordTooLargeException |
                   _: RecordBatchTooLargeException |
                   _: CorruptRecordException |
                   _: KafkaStorageException) =>
            (topicPartition, LogAppendResult(LogAppendInfo.UnknownLogAppendInfo, Some(e)))
          case rve: RecordValidationException =>
            val logStartOffset = processFailedRecord(topicPartition, rve.invalidException)
            val recordErrors = rve.recordErrors
            (topicPartition, LogAppendResult(LogAppendInfo.unknownLogAppendInfoWithAdditionalInfo(
              logStartOffset, recordErrors, rve.invalidException.getMessage), Some(rve.invalidException)))
          case t: Throwable =>
            val logStartOffset = processFailedRecord(topicPartition, t)
            (topicPartition, LogAppendResult(LogAppendInfo.unknownLogAppendInfoWithLogStartOffset(logStartOffset), Some(t)))
        }
      }
    }
  }

  def fetchOffsetForTimestamp(topicPartition: TopicPartition,
                              timestamp: Long,
                              isolationLevel: Option[IsolationLevel],
                              currentLeaderEpoch: Optional[Integer],
                              fetchOnlyFromLeader: Boolean): Option[TimestampAndOffset] = {
    val partition = onlinePartitionOrException(topicPartition)
    partition.fetchOffsetForTimestamp(timestamp, isolationLevel, currentLeaderEpoch, fetchOnlyFromLeader)
  }

  def legacyFetchOffsetsForTimestamp(topicPartition: TopicPartition,
                                     timestamp: Long,
                                     maxNumOffsets: Int,
                                     isFromConsumer: Boolean,
                                     fetchOnlyFromLeader: Boolean): Seq[Long] = {
    val partition = onlinePartitionOrException(topicPartition)
    partition.legacyFetchOffsetsForTimestamp(timestamp, maxNumOffsets, isFromConsumer, fetchOnlyFromLeader)
  }

  /**
   * Fetch messages from a replica, and wait until enough data can be fetched and return;
   * the callback function will be triggered either when timeout or required fetch info is satisfied.
   * Consumers may fetch from any replica, but followers can only fetch from the leader.
   */
  def fetchMessages(timeout: Long,
                    replicaId: Int,
                    fetchMinBytes: Int,
                    fetchMaxBytes: Int,
                    hardMaxBytesLimit: Boolean,
                    fetchInfos: Seq[(TopicPartition, PartitionData)],
                    quota: ReplicaQuota,
                    responseCallback: Seq[(TopicPartition, FetchPartitionData)] => Unit,
                    isolationLevel: IsolationLevel,
                    clientMetadata: Option[ClientMetadata]): Unit = {
    val isFromFollower = Request.isValidBrokerId(replicaId)
    val isFromConsumer = !(isFromFollower || replicaId == Request.FutureLocalReplicaId)
    val fetchIsolation = if (!isFromConsumer)
      FetchLogEnd
    else if (isolationLevel == IsolationLevel.READ_COMMITTED)
      FetchTxnCommitted
    else
      FetchHighWatermark

    // Restrict fetching to leader if request is from follower or from a client with older version (no ClientMetadata)
    val fetchOnlyFromLeader = isFromFollower || (isFromConsumer && clientMetadata.isEmpty)
    def readFromLog(): Seq[(TopicPartition, LogReadResult)] = {
      val result = readFromLocalLog(
        replicaId = replicaId,
        fetchOnlyFromLeader = fetchOnlyFromLeader,
        fetchIsolation = fetchIsolation,
        fetchMaxBytes = fetchMaxBytes,
        hardMaxBytesLimit = hardMaxBytesLimit,
        readPartitionInfo = fetchInfos,
        quota = quota,
        clientMetadata = clientMetadata)
      if (isFromFollower) updateFollowerFetchState(replicaId, result)
      else result
    }

    val logReadResults = readFromLog()

    // check if this fetch request can be satisfied right away
    var bytesReadable: Long = 0
    var errorReadingData = false
    var hasDivergingEpoch = false
    val logReadResultMap = new mutable.HashMap[TopicPartition, LogReadResult]
    logReadResults.foreach { case (topicPartition, logReadResult) =>
      brokerTopicStats.topicStats(topicPartition.topic).totalFetchRequestRate.mark()
      brokerTopicStats.allTopicsStats.totalFetchRequestRate.mark()

      if (logReadResult.error != Errors.NONE)
        errorReadingData = true
      if (logReadResult.divergingEpoch.nonEmpty)
        hasDivergingEpoch = true
      bytesReadable = bytesReadable + logReadResult.info.records.sizeInBytes
      logReadResultMap.put(topicPartition, logReadResult)
    }

    // respond immediately if 1) fetch request does not want to wait
    //                        2) fetch request does not require any data
    //                        3) has enough data to respond
    //                        4) some error happens while reading data
    //                        5) we found a diverging epoch
    if (timeout <= 0 || fetchInfos.isEmpty || bytesReadable >= fetchMinBytes || errorReadingData || hasDivergingEpoch) {
      val fetchPartitionData = logReadResults.map { case (tp, result) =>
        val isReassignmentFetch = isFromFollower && isAddingReplica(tp, replicaId)
        tp -> FetchPartitionData(
          result.error,
          result.highWatermark,
          result.leaderLogStartOffset,
          result.info.records,
          result.divergingEpoch,
          result.lastStableOffset,
          result.info.abortedTransactions,
          result.preferredReadReplica,
          isReassignmentFetch)
      }
      responseCallback(fetchPartitionData)
    } else {
      // construct the fetch results from the read results
      val fetchPartitionStatus = new mutable.ArrayBuffer[(TopicPartition, FetchPartitionStatus)]
      fetchInfos.foreach { case (topicPartition, partitionData) =>
        logReadResultMap.get(topicPartition).foreach(logReadResult => {
          val logOffsetMetadata = logReadResult.info.fetchOffsetMetadata
          fetchPartitionStatus += (topicPartition -> FetchPartitionStatus(logOffsetMetadata, partitionData))
        })
      }
      val fetchMetadata: SFetchMetadata = SFetchMetadata(fetchMinBytes, fetchMaxBytes, hardMaxBytesLimit,
        fetchOnlyFromLeader, fetchIsolation, isFromFollower, replicaId, fetchPartitionStatus)
      val delayedFetch = new DelayedFetch(timeout, fetchMetadata, this, quota, clientMetadata,
        responseCallback)

      // create a list of (topic, partition) pairs to use as keys for this delayed fetch operation
      val delayedFetchKeys = fetchPartitionStatus.map { case (tp, _) => TopicPartitionOperationKey(tp) }

      // try to complete the request immediately, otherwise put it into the purgatory;
      // this is because while the delayed fetch operation is being created, new requests
      // may arrive and hence make this operation completable.
      delayedFetchPurgatory.tryCompleteElseWatch(delayedFetch, delayedFetchKeys)
    }
  }

  /**
   * Read from multiple topic partitions at the given offset up to maxSize bytes
   */
  def readFromLocalLog(replicaId: Int,
                       fetchOnlyFromLeader: Boolean,
                       fetchIsolation: FetchIsolation,
                       fetchMaxBytes: Int,
                       hardMaxBytesLimit: Boolean,
                       readPartitionInfo: Seq[(TopicPartition, PartitionData)],
                       quota: ReplicaQuota,
                       clientMetadata: Option[ClientMetadata]): Seq[(TopicPartition, LogReadResult)] = {
    val traceEnabled = isTraceEnabled

    def read(tp: TopicPartition, fetchInfo: PartitionData, limitBytes: Int, minOneMessage: Boolean): LogReadResult = {
      val offset = fetchInfo.fetchOffset
      val partitionFetchSize = fetchInfo.maxBytes
      val followerLogStartOffset = fetchInfo.logStartOffset

      val adjustedMaxBytes = math.min(fetchInfo.maxBytes, limitBytes)
      try {
        if (traceEnabled)
          trace(s"Fetching log segment for partition $tp, offset $offset, partition fetch size $partitionFetchSize, " +
            s"remaining response limit $limitBytes" +
            (if (minOneMessage) s", ignoring response/partition size limits" else ""))

        val partition = onlinePartitionOrException(tp)
        val fetchTimeMs = time.milliseconds

        // If we are the leader, determine the preferred read-replica
        val preferredReadReplica = clientMetadata.flatMap(
          metadata => findPreferredReadReplica(partition, metadata, replicaId, fetchInfo.fetchOffset, fetchTimeMs))

        if (preferredReadReplica.isDefined) {
          replicaSelectorOpt.foreach { selector =>
            debug(s"Replica selector ${selector.getClass.getSimpleName} returned preferred replica " +
              s"${preferredReadReplica.get} for $clientMetadata")
          }
          // If a preferred read-replica is set, skip the read
          val offsetSnapshot = partition.fetchOffsetSnapshot(fetchInfo.currentLeaderEpoch, fetchOnlyFromLeader = false)
          LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
            divergingEpoch = None,
            highWatermark = offsetSnapshot.highWatermark.messageOffset,
            leaderLogStartOffset = offsetSnapshot.logStartOffset,
            leaderLogEndOffset = offsetSnapshot.logEndOffset.messageOffset,
            followerLogStartOffset = followerLogStartOffset,
            fetchTimeMs = -1L,
            lastStableOffset = Some(offsetSnapshot.lastStableOffset.messageOffset),
            preferredReadReplica = preferredReadReplica,
            exception = None)
        } else {
          // Try the read first, this tells us whether we need all of adjustedFetchSize for this partition
          val readInfo: LogReadInfo = partition.readRecords(
            lastFetchedEpoch = fetchInfo.lastFetchedEpoch,
            fetchOffset = fetchInfo.fetchOffset,
            currentLeaderEpoch = fetchInfo.currentLeaderEpoch,
            maxBytes = adjustedMaxBytes,
            fetchIsolation = fetchIsolation,
            fetchOnlyFromLeader = fetchOnlyFromLeader,
            minOneMessage = minOneMessage)

          val fetchDataInfo = if (shouldLeaderThrottle(quota, partition, replicaId)) {
            // If the partition is being throttled, simply return an empty set.
            FetchDataInfo(readInfo.fetchedData.fetchOffsetMetadata, MemoryRecords.EMPTY)
          } else if (!hardMaxBytesLimit && readInfo.fetchedData.firstEntryIncomplete) {
            // For FetchRequest version 3, we replace incomplete message sets with an empty one as consumers can make
            // progress in such cases and don't need to report a `RecordTooLargeException`
            FetchDataInfo(readInfo.fetchedData.fetchOffsetMetadata, MemoryRecords.EMPTY)
          } else {
            readInfo.fetchedData
          }

          LogReadResult(info = fetchDataInfo,
            divergingEpoch = readInfo.divergingEpoch,
            highWatermark = readInfo.highWatermark,
            leaderLogStartOffset = readInfo.logStartOffset,
            leaderLogEndOffset = readInfo.logEndOffset,
            followerLogStartOffset = followerLogStartOffset,
            fetchTimeMs = fetchTimeMs,
            lastStableOffset = Some(readInfo.lastStableOffset),
            preferredReadReplica = preferredReadReplica,
            exception = None)
        }
      } catch {
        // NOTE: Failed fetch requests metric is not incremented for known exceptions since it
        // is supposed to indicate un-expected failure of a broker in handling a fetch request
        case e@ (_: UnknownTopicOrPartitionException |
                 _: NotLeaderOrFollowerException |
                 _: UnknownLeaderEpochException |
                 _: FencedLeaderEpochException |
                 _: ReplicaNotAvailableException |
                 _: KafkaStorageException |
                 _: OffsetOutOfRangeException) =>
          LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
            divergingEpoch = None,
            highWatermark = Log.UnknownOffset,
            leaderLogStartOffset = Log.UnknownOffset,
            leaderLogEndOffset = Log.UnknownOffset,
            followerLogStartOffset = Log.UnknownOffset,
            fetchTimeMs = -1L,
            lastStableOffset = None,
            exception = Some(e))
        case e: Throwable =>
          brokerTopicStats.topicStats(tp.topic).failedFetchRequestRate.mark()
          brokerTopicStats.allTopicsStats.failedFetchRequestRate.mark()

          val fetchSource = Request.describeReplicaId(replicaId)
          error(s"Error processing fetch with max size $adjustedMaxBytes from $fetchSource " +
            s"on partition $tp: $fetchInfo", e)

          LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
            divergingEpoch = None,
            highWatermark = Log.UnknownOffset,
            leaderLogStartOffset = Log.UnknownOffset,
            leaderLogEndOffset = Log.UnknownOffset,
            followerLogStartOffset = Log.UnknownOffset,
            fetchTimeMs = -1L,
            lastStableOffset = None,
            exception = Some(e))
      }
    }

    var limitBytes = fetchMaxBytes
    val result = new mutable.ArrayBuffer[(TopicPartition, LogReadResult)]
    var minOneMessage = !hardMaxBytesLimit
    readPartitionInfo.foreach { case (tp, fetchInfo) =>
      val readResult = read(tp, fetchInfo, limitBytes, minOneMessage)
      val recordBatchSize = readResult.info.records.sizeInBytes
      // Once we read from a non-empty partition, we stop ignoring request and partition level size limits
      if (recordBatchSize > 0)
        minOneMessage = false
      limitBytes = math.max(0, limitBytes - recordBatchSize)
      result += (tp -> readResult)
    }
    result
  }

  /**
    * Using the configured [[ReplicaSelector]], determine the preferred read replica for a partition given the
    * client metadata, the requested offset, and the current set of replicas. If the preferred read replica is the
    * leader, return None
    */
  def findPreferredReadReplica(partition: Partition,
                               clientMetadata: ClientMetadata,
                               replicaId: Int,
                               fetchOffset: Long,
                               currentTimeMs: Long): Option[Int] = {
    partition.leaderReplicaIdOpt.flatMap { leaderReplicaId =>
      // Don't look up preferred for follower fetches via normal replication
      if (Request.isValidBrokerId(replicaId))
        None
      else {
        replicaSelectorOpt.flatMap { replicaSelector =>
          val replicaEndpoints = metadataCache.getPartitionReplicaEndpoints(partition.topicPartition,
            new ListenerName(clientMetadata.listenerName))
          val replicaInfos = partition.remoteReplicas
            // Exclude replicas that don't have the requested offset (whether or not if they're in the ISR)
            .filter(replica => replica.logEndOffset >= fetchOffset && replica.logStartOffset <= fetchOffset)
            .map(replica => new DefaultReplicaView(
              replicaEndpoints.getOrElse(replica.brokerId, Node.noNode()),
              replica.logEndOffset,
              currentTimeMs - replica.lastCaughtUpTimeMs))

          val leaderReplica = new DefaultReplicaView(
            replicaEndpoints.getOrElse(leaderReplicaId, Node.noNode()),
            partition.localLogOrException.logEndOffset, 0L)
          val replicaInfoSet = mutable.Set[ReplicaView]() ++= replicaInfos += leaderReplica

          val partitionInfo = new DefaultPartitionView(replicaInfoSet.asJava, leaderReplica)
          replicaSelector.select(partition.topicPartition, clientMetadata, partitionInfo).asScala.collect {
            // Even though the replica selector can return the leader, we don't want to send it out with the
            // FetchResponse, so we exclude it here
            case selected if !selected.endpoint.isEmpty && selected != leaderReplica => selected.endpoint.id
          }
        }
      }
    }
  }

  /**
   *  To avoid ISR thrashing, we only throttle a replica on the leader if it's in the throttled replica list,
   *  the quota is exceeded and the replica is not in sync.
   */
  def shouldLeaderThrottle(quota: ReplicaQuota, partition: Partition, replicaId: Int): Boolean = {
    val isReplicaInSync = partition.inSyncReplicaIds.contains(replicaId)
    !isReplicaInSync && quota.isThrottled(partition.topicPartition) && quota.isQuotaExceeded
  }

  /**
   * Handle changes made by a batch of KIP-500 metadata records.
   *
   * @param imageBuilder        The MetadataImage builder.
   * @param metadataOffset      The last offset in the batch of records.
   * @param onLeadershipChange  The callbacks to invoke when leadership changes.
   */
  def handleMetadataRecords(imageBuilder: MetadataImageBuilder,
                            metadataOffset: Long,
                            onLeadershipChange: (Iterable[Partition], Iterable[Partition]) => Unit): Unit = {
    val builder = imageBuilder.partitionsBuilder()
    val startMs = time.milliseconds()
    replicaStateChangeLock synchronized {
      val deferrable = changesDeferrable
      stateChangeLogger.info(("Metadata batch %d: %d local partition(s) changed, %d " +
        "local partition(s) removed.").format(metadataOffset, builder.localChanged().size,
          builder.localRemoved().size))
      if (stateChangeLogger.isTraceEnabled) {
        builder.localChanged().foreach { state =>
          stateChangeLogger.trace(s"Metadata batch $metadataOffset: locally changed: ${state}")
        }
        builder.localRemoved().foreach { state =>
          stateChangeLogger.trace(s"Metadata batch $metadataOffset: locally removed: ${state}")
        }
      }
      // First create the partition if it doesn't exist already
      // partitionChangesToBeDeferred maps each partition to be deferred to its (current state, previous deferred state if any)
      val partitionChangesToBeDeferred = mutable.HashMap[Partition, (MetadataPartition, Option[HostedPartitionRaft.Deferred])]()
      val partitionsToBeLeader = mutable.HashMap[Partition, MetadataPartition]()
      val partitionsToBeFollower = mutable.HashMap[Partition, MetadataPartition]()
      builder.localChanged().foreach { state =>
        val topicPartition = new TopicPartition(state.topicName, state.partitionIndex)
        val (partition, priorDeferredMetadata) = getPartition(topicPartition) match {
          case HostedPartitionRaft.Offline =>
            stateChangeLogger.warn(s"Ignoring handlePartitionChanges at $metadataOffset " +
              s"for partition $topicPartition as the local replica for the partition is " +
              "in an offline log directory")
            (None, None)

          case HostedPartitionRaft.Online(partition) => (Some(partition), None)
          case deferred@HostedPartitionRaft.Deferred(partition, _, _, _, _) => (Some(partition), Some(deferred))

          case HostedPartitionRaft.None =>
            val partition = Partition(topicPartition, time, this)
            if (!deferrable) {
              allPartitions.putIfNotExists(topicPartition, HostedPartitionRaft.Online(partition))
            }
            (Some(partition), None)
        }
        partition.foreach { partition =>
          val alreadyDeferred = priorDeferredMetadata.nonEmpty
          if (alreadyDeferred || deferrable && partition.getLeaderEpoch != state.leaderEpoch) {
            partitionChangesToBeDeferred.put(partition, (state, priorDeferredMetadata))
          } else if (state.leaderId == localBrokerId) {
            partitionsToBeLeader.put(partition, state)
          } else {
            partitionsToBeFollower.put(partition, state)
          }
        }
      }
      val prevPartitions = imageBuilder.prevImage.partitions
      val changedPartitionsPreviouslyExisting = mutable.Set[MetadataPartition]()
      builder.localChanged().foreach(metadataPartition =>
        prevPartitions.get(metadataPartition.topicName, metadataPartition.partitionIndex).foreach(
          changedPartitionsPreviouslyExisting.add))
      val nextBrokers = imageBuilder.nextBrokers()
      val highWatermarkCheckpoints = new LazyOffsetCheckpoints(this.highWatermarkCheckpoints)
      val partitionsBecomeLeader = if (partitionsToBeLeader.nonEmpty)
        makeLeaders(changedPartitionsPreviouslyExisting, partitionsToBeLeader,
          highWatermarkCheckpoints, metadataOffset)
      else
        Set.empty[Partition]
      val partitionsBecomeFollower = if (partitionsToBeFollower.nonEmpty)
        makeFollowers(changedPartitionsPreviouslyExisting, nextBrokers, partitionsToBeFollower, highWatermarkCheckpoints,
          metadataOffset)
      else {
        Set.empty[Partition]
      }
      stateChangeLogger.info(s"Deferring metadata changes for ${partitionChangesToBeDeferred.size} partition(s)")
      if (partitionChangesToBeDeferred.nonEmpty) {
        makeDeferred(imageBuilder, partitionChangesToBeDeferred, metadataOffset, onLeadershipChange)
      }

      helper.updateLeaderAndFollowerMetrics(this, partitionsBecomeFollower.map(_.topic).toSet)

      builder.localChanged().foreach { state =>
        val topicPartition = new TopicPartition(state.topicName, state.partitionIndex)
        /*
         * If there is offline log directory, a Partition object may have been created by getOrCreatePartition()
         * before getOrCreateReplica() failed to create local replica due to KafkaStorageException.
         * In this case ReplicaManager.allPartitions will map this topic-partition to an empty Partition object.
         * we need to map this topic-partition to OfflinePartition instead.
         */
        // only mark it offline if it isn't deferred
        if (localOnlineLog(topicPartition).isEmpty && !isDeferred(topicPartition)) {
          markPartitionOffline(topicPartition)
        }
      }

      helper.maybeAddLogDirFetchers(this, partitionsBecomeFollower, highWatermarkCheckpoints)

      replicaFetcherManager.shutdownIdleFetcherThreads()
      replicaAlterLogDirsManager.shutdownIdleFetcherThreads()
      onLeadershipChange(partitionsBecomeLeader, partitionsBecomeFollower)

      // TODO: we should move aside log directories which have been deleted rather than
      // purging them from the disk immediately.
      if (builder.localRemoved().nonEmpty) {
        // we remove immediately even if we are deferring changes
        stopPartitions(builder.localRemoved().map(_.toTopicPartition() -> true).toMap).foreach {
          case (topicPartition, e) => if (e.isInstanceOf[KafkaStorageException]) {
            stateChangeLogger.error(s"Metadata batch $metadataOffset: unable to delete " +
              s"${topicPartition} as the local replica for the partition is in an offline " +
              "log directory")
          } else {
            stateChangeLogger.error(s"Metadata batch $metadataOffset: unable to delete " +
              s"${topicPartition} due to an unexpected ${e.getClass.getName} exception: " +
              s"${e.getMessage}")
          }
        }
      }
    }
    val endMs = time.milliseconds()
    val elapsedMs = endMs - startMs
    stateChangeLogger.info(s"Metadata batch $metadataOffset: handled replica changes " +
      s"in ${elapsedMs} ms")
  }

  private def makeLeaders(prevPartitionsAlreadyExisting: Set[MetadataPartition],
                          partitionStates: Map[Partition, MetadataPartition],
                          highWatermarkCheckpoints: OffsetCheckpoints,
                          defaultMetadataOffset: Long,
                          metadataOffsets: Map[Partition, Long] = Map.empty): Set[Partition] = {
    val partitionsMadeLeaders = mutable.Set[Partition]()
    val traceLoggingEnabled = stateChangeLogger.isTraceEnabled
    val deferredBatches = metadataOffsets.nonEmpty
    val topLevelLogPrefix = if (deferredBatches)
      "Metadata batch <multiple deferred>"
    else
      s"Metadata batch $defaultMetadataOffset"
    try {
      // First stop fetchers for all the partitions
      replicaFetcherManager.removeFetcherForPartitions(partitionStates.keySet.map(_.topicPartition))
      stateChangeLogger.info(s"$topLevelLogPrefix: stopped ${partitionStates.size} fetcher(s)")
      // Update the partition information to be the leader
      partitionStates.forKeyValue { (partition, state) =>
        val metadataOffset = metadataOffsets.getOrElse(partition, defaultMetadataOffset)
        val topicPartition = partition.topicPartition
        val partitionLogMsgPrefix = if (deferredBatches)
          s"Apply deferred leader partition $topicPartition last seen in metadata batch $metadataOffset"
        else
          s"Metadata batch $metadataOffset $topicPartition"
        try {
          val isrState = state.toLeaderAndIsrPartitionState(
            !prevPartitionsAlreadyExisting(state))
          if (partition.makeLeader(isrState, highWatermarkCheckpoints)) {
            partitionsMadeLeaders += partition
            if (traceLoggingEnabled) {
              stateChangeLogger.trace(s"$partitionLogMsgPrefix: completed the become-leader state change.")
            }
          } else {
            stateChangeLogger.info(s"$partitionLogMsgPrefix: skipped the " +
              "become-leader state change since it is already the leader.")
          }
        } catch {
          case e: KafkaStorageException =>
            stateChangeLogger.error(s"$partitionLogMsgPrefix: unable to make " +
              s"leader because the replica for the partition is offline due to disk error $e")
            val dirOpt = getLogDir(topicPartition)
            error(s"Error while making broker the leader for partition $partition in dir $dirOpt", e)
            markPartitionOffline(topicPartition)
        }
      }
    } catch {
      case e: Throwable =>
          stateChangeLogger.error(s"$topLevelLogPrefix: error while processing batch.", e)
        // Re-throw the exception for it to be caught in BrokerMetadataListener
        throw e
    }
    partitionsMadeLeaders
  }

  private def makeFollowers(prevPartitionsAlreadyExisting: Set[MetadataPartition],
                            currentBrokers: MetadataBrokers,
                            partitionStates: Map[Partition, MetadataPartition],
                            highWatermarkCheckpoints: OffsetCheckpoints,
                            defaultMetadataOffset: Long,
                            metadataOffsets: Map[Partition, Long] = Map.empty): Set[Partition] = {
    val traceLoggingEnabled = stateChangeLogger.isTraceEnabled
    val deferredBatches = metadataOffsets.nonEmpty
    val topLevelLogPrefix = if (deferredBatches)
      "Metadata batch <multiple deferred>"
    else
      s"Metadata batch $defaultMetadataOffset"
    if (traceLoggingEnabled) {
      partitionStates.forKeyValue { (partition, state) =>
        val metadataOffset = metadataOffsets.getOrElse(partition, defaultMetadataOffset)
        val topicPartition = partition.topicPartition
        val partitionLogMsgPrefix = if (deferredBatches)
          s"Apply deferred follower partition $topicPartition last seen in metadata batch $metadataOffset"
        else
          s"Metadata batch $metadataOffset $topicPartition"
        stateChangeLogger.trace(s"$partitionLogMsgPrefix: starting the " +
          s"become-follower transition with leader ${state.leaderId}")
      }
    }

    val partitionsMadeFollower: mutable.Set[Partition] = mutable.Set()
    // all brokers, including both alive and not
    val acceptableLeaderBrokerIds = currentBrokers.iterator().map(broker => broker.id).toSet
    val allBrokersByIdMap = currentBrokers.iterator().map(broker => broker.id -> broker).toMap
    try {
      partitionStates.forKeyValue { (partition, state) =>
        val metadataOffset = metadataOffsets.getOrElse(partition, defaultMetadataOffset)
        val topicPartition = partition.topicPartition
        val partitionLogMsgPrefix = if (deferredBatches)
          s"Apply deferred follower partition $topicPartition last seen in metadata batch $metadataOffset"
        else
          s"Metadata batch $metadataOffset $topicPartition"
        try {
          val isNew = !prevPartitionsAlreadyExisting(state)
          if (!acceptableLeaderBrokerIds.contains(state.leaderId)) {
            // The leader broker should always be present in the metadata cache.
            // If not, we should record the error message and abort the transition process for this partition
            stateChangeLogger.error(s"$partitionLogMsgPrefix: cannot become follower " +
              s"since the new leader ${state.leaderId} is unavailable.")
            // Create the local replica even if the leader is unavailable. This is required to ensure that we include
            // the partition's high watermark in the checkpoint file (see KAFKA-1647)
            partition.createLogIfNotExists(isNew, isFutureReplica = false, highWatermarkCheckpoints)
          } else {
            val isrState = state.toLeaderAndIsrPartitionState(isNew)
            if (partition.makeFollower(isrState, highWatermarkCheckpoints)) {
              partitionsMadeFollower += partition
              if (traceLoggingEnabled) {
                stateChangeLogger.trace(s"$partitionLogMsgPrefix: completed the " +
                  s"become-follower state change with new leader ${state.leaderId}.")
              }
            } else {
              stateChangeLogger.info(s"$partitionLogMsgPrefix: skipped the " +
                s"become-follower state change since " +
                s"the new leader ${state.leaderId} is the same as the old leader.")
            }
          }
        } catch {
          case e: KafkaStorageException =>
            stateChangeLogger.error(s"$partitionLogMsgPrefix: unable to complete the " +
              s"become-follower state change since the " +
              s"replica for the partition is offline due to disk error $e")
            val dirOpt = getLogDir(partition.topicPartition)
            error(s"Error while making broker the follower with leader ${state.leaderId} in dir $dirOpt", e)
            markPartitionOffline(topicPartition)
        }
      }

      if (partitionsMadeFollower.nonEmpty) {
        replicaFetcherManager.removeFetcherForPartitions(partitionsMadeFollower.map(_.topicPartition))
        stateChangeLogger.info(s"$topLevelLogPrefix: stopped followers for ${partitionsMadeFollower.size} partitions")

        partitionsMadeFollower.foreach { partition =>
          helper.completeDelayedFetchOrProduceRequests(this, partition.topicPartition)
        }

        if (isShuttingDown.get()) {
          if (traceLoggingEnabled) {
            partitionsMadeFollower.foreach { partition =>
              val metadataOffset = metadataOffsets.getOrElse(partition, defaultMetadataOffset)
              val topicPartition = partition.topicPartition
              val partitionLogMsgPrefix = if (deferredBatches)
                s"Apply deferred follower partition $topicPartition last seen in metadata batch $metadataOffset"
              else
                s"Metadata batch $metadataOffset $topicPartition"
              stateChangeLogger.trace(s"$partitionLogMsgPrefix: skipped the " +
                s"adding-fetcher step of the become-follower state for " +
                s"$topicPartition since we are shutting down.")
            }
          }
        } else {
          // we do not need to check if the leader exists again since this has been done at the beginning of this process
          val partitionsToMakeFollowerWithLeaderAndOffset = partitionsMadeFollower.map { partition =>
            val leader = allBrokersByIdMap(partition.leaderReplicaIdOpt.get).brokerEndPoint(config.interBrokerListenerName)
            val log = partition.localLogOrException
            val fetchOffset = initialFetchOffset(log)
            partition.topicPartition -> InitialFetchState(leader, partition.getLeaderEpoch, fetchOffset)
          }.toMap

          replicaFetcherManager.addFetcherForPartitions(partitionsToMakeFollowerWithLeaderAndOffset)
        }
      }
    } catch {
      case e: Throwable =>
        stateChangeLogger.error(s"$topLevelLogPrefix: error while processing batch", e)
        // Re-throw the exception for it to be caught in BrokerMetadataListener
        throw e
    }

    if (traceLoggingEnabled)
      partitionsMadeFollower.foreach { partition =>
        val metadataOffset = metadataOffsets.getOrElse(partition, defaultMetadataOffset)
        val topicPartition = partition.topicPartition
        val partitionLogMsgPrefix = if (deferredBatches)
          s"Apply deferred follower partition $topicPartition last seen in metadata batch $metadataOffset"
        else
          s"Metadata batch $metadataOffset $topicPartition"
        val state = partitionStates(partition)
        stateChangeLogger.trace(s"$partitionLogMsgPrefix: completed become-follower " +
          s"transition for partition $topicPartition with new leader ${state.leaderId}")
      }

    partitionsMadeFollower
  }

  private def makeDeferred(builder: MetadataImageBuilder,
                           partitionStates: Map[Partition, (MetadataPartition, Option[HostedPartitionRaft.Deferred])],
                           metadataOffset: Long,
                           onLeadershipChange: (Iterable[Partition], Iterable[Partition]) => Unit) : Unit = {
    val traceLoggingEnabled = stateChangeLogger.isTraceEnabled
    if (traceLoggingEnabled)
      partitionStates.forKeyValue { (partition, stateAndMetadata) =>
        stateChangeLogger.trace(s"Metadata batch $metadataOffset: starting the " +
          s"become-deferred transition for partition ${partition.topicPartition} with leader " +
          s"${stateAndMetadata._1.leaderId}")
      }

    // Stop fetchers for all the partitions
    replicaFetcherManager.removeFetcherForPartitions(partitionStates.keySet.map(_.topicPartition))
    stateChangeLogger.info(s"Metadata batch $metadataOffset: as part of become-deferred request, " +
      s"stopped any fetchers for ${partitionStates.size} partitions")
    val prevPartitions = builder.prevImage.partitions
    partitionStates.forKeyValue { (partition, currentAndOptionalPreviousDeferredState) =>
      val currentState = currentAndOptionalPreviousDeferredState._1
      val latestDeferredPartitionState = currentAndOptionalPreviousDeferredState._2
      val isNew = prevPartitions.get(currentState.topicName, currentState.partitionIndex).isEmpty ||
        latestDeferredPartitionState.isDefined && latestDeferredPartitionState.get.wasNew
      allPartitions.put(partition.topicPartition,
        HostedPartitionRaft.Deferred(partition, currentState, isNew, metadataOffset, onLeadershipChange))
    }

    if (traceLoggingEnabled)
      partitionStates.keys.foreach { partition =>
        stateChangeLogger.trace(s"Completed batch $metadataOffset become-deferred " +
          s"transition for partition ${partition.topicPartition} with new leader " +
          s"${partitionStates(partition)._1.leaderId}")
      }
  }

  /**
   * From IBP 2.7 onwards, we send latest fetch epoch in the request and truncate if a
   * diverging epoch is returned in the response, avoiding the need for a separate
   * OffsetForLeaderEpoch request.
   */
  private def initialFetchOffset(log: Log): Long = {
    if (ApiVersion.isTruncationOnFetchSupported(config.interBrokerProtocolVersion) && log.latestEpoch.nonEmpty)
      log.logEndOffset
    else
      log.highWatermark
  }

  private def maybeShrinkIsr(): Unit = {
    trace("Evaluating ISR list of partitions to see which replicas can be removed from the ISR")

    // Shrink ISRs for non offline partitions
    allPartitions.keys.foreach { topicPartition =>
      onlinePartition(topicPartition).foreach(_.maybeShrinkIsr())
    }
  }

  /**
   * Update the follower's fetch state on the leader based on the last fetch request and update `readResult`.
   * If the follower replica is not recognized to be one of the assigned replicas, do not update
   * `readResult` so that log start/end offset and high watermark is consistent with
   * records in fetch response. Log start/end offset and high watermark may change not only due to
   * this fetch request, e.g., rolling new log segment and removing old log segment may move log
   * start offset further than the last offset in the fetched records. The followers will get the
   * updated leader's state in the next fetch response.
   */
  private def updateFollowerFetchState(followerId: Int,
                                       readResults: Seq[(TopicPartition, LogReadResult)]): Seq[(TopicPartition, LogReadResult)] = {
    readResults.map { case (topicPartition, readResult) =>
      val updatedReadResult = if (readResult.error != Errors.NONE) {
        debug(s"Skipping update of fetch state for follower $followerId since the " +
          s"log read returned error ${readResult.error}")
        readResult
      } else {
        deferredOrOnlinePartition(topicPartition) match {
          case Some(partition) =>
            if (partition.updateFollowerFetchState(followerId,
              followerFetchOffsetMetadata = readResult.info.fetchOffsetMetadata,
              followerStartOffset = readResult.followerLogStartOffset,
              followerFetchTimeMs = readResult.fetchTimeMs,
              leaderEndOffset = readResult.leaderLogEndOffset)) {
              readResult
            } else {
              warn(s"Leader $localBrokerId failed to record follower $followerId's position " +
                s"${readResult.info.fetchOffsetMetadata.messageOffset}, and last sent HW since the replica " +
                s"is not recognized to be one of the assigned replicas ${partition.assignmentState.replicas.mkString(",")} " +
                s"for partition $topicPartition. Empty records will be returned for this partition.")
              readResult.withEmptyFetchInfo
            }
          case None =>
            warn(s"While recording the replica LEO, the partition $topicPartition hasn't been created.")
            readResult
        }
      }
      topicPartition -> updatedReadResult
    }
  }

  def checkpointHighWatermarks(): Unit = {
    helper.checkpointHighWatermarks(this, () => highWatermarkCheckpoints)
  }

  def markPartitionOffline(tp: TopicPartition): Unit = replicaStateChangeLock synchronized {
    allPartitions.put(tp, HostedPartitionRaft.Offline)
    Partition.removeMetrics(tp)
  }

  /**
   * The log directory failure handler for the replica
   *
   * @param dir                     the absolute path of the log directory
   * @param sendZkNotification      check if we need to send notification to zookeeper node (needed for unit test)
   */
  def handleLogDirFailure(dir: String, sendZkNotification: Boolean = true): Unit = {
    if (!logManager.isLogDirOnline(dir))
      return
    warn(s"Stopping serving replicas in dir $dir")
    replicaStateChangeLock synchronized {
      val newOfflinePartitions = deferredOrOnlinePartitionsIterator.filter { partition =>
        partition.log.exists { _.parentDir == dir }
      }.map(_.topicPartition).toSet

      val partitionsWithOfflineFutureReplica = deferredOrOnlinePartitionsIterator.filter { partition =>
        partition.futureLog.exists { _.parentDir == dir }
      }.toSet

      replicaFetcherManager.removeFetcherForPartitions(newOfflinePartitions)
      replicaAlterLogDirsManager.removeFetcherForPartitions(newOfflinePartitions ++ partitionsWithOfflineFutureReplica.map(_.topicPartition))

      partitionsWithOfflineFutureReplica.foreach(partition => partition.removeFutureLocalReplica(deleteFromLogDir = false))
      newOfflinePartitions.foreach { topicPartition =>
        markPartitionOffline(topicPartition)
      }
      newOfflinePartitions.map(_.topic).foreach { topic: String =>
        maybeRemoveTopicMetrics(topic)
      }
      highWatermarkCheckpoints = highWatermarkCheckpoints.filter { case (checkpointDir, _) => checkpointDir != dir }

      warn(s"Broker $localBrokerId stopped fetcher for partitions ${newOfflinePartitions.mkString(",")} and stopped moving logs " +
           s"for partitions ${partitionsWithOfflineFutureReplica.mkString(",")} because they are in the failed log directory $dir.")
    }
    logManager.handleLogDirFailure(dir)

    if (sendZkNotification) {
      if (zkClient.isEmpty) {
        warn("Unable to propagate log dir failure via Zookeeper in KIP-500 mode") // will be handled via KIP-589
      } else {
        zkClient.get.propagateLogDirEvent(localBrokerId)
      }
    }
    warn(s"Stopped serving replicas in dir $dir")
  }

  def removeMetrics(): Unit = {
    removeMetric("LeaderCount")
    removeMetric("PartitionCount")
    removeMetric("OfflineReplicaCount")
    removeMetric("UnderReplicatedPartitions")
    removeMetric("UnderMinIsrPartitionCount")
    removeMetric("AtMinIsrPartitionCount")
  }

  // High watermark do not need to be checkpointed only when under unit tests
  def shutdown(checkpointHW: Boolean = true): Unit = {
    info("Shutting down")
    removeMetrics()
    if (logDirFailureHandler != null)
      logDirFailureHandler.shutdown()
    replicaFetcherManager.shutdown()
    replicaAlterLogDirsManager.shutdown()
    delayedFetchPurgatory.shutdown()
    delayedProducePurgatory.shutdown()
    delayedDeleteRecordsPurgatory.shutdown()
    delayedElectLeaderPurgatory.shutdown()
    if (checkpointHW)
      checkpointHighWatermarks()
    replicaSelectorOpt.foreach(_.close)
    info("Shut down completely")
  }

  protected def createReplicaFetcherManager(metrics: Metrics, time: Time, threadNamePrefix: Option[String], quotaManager: ReplicationQuotaManager) = {
    new ReplicaFetcherManager(config, this, metrics, time, threadNamePrefix, quotaManager)
  }

  protected def createReplicaAlterLogDirsManager(quotaManager: ReplicationQuotaManager, brokerTopicStats: BrokerTopicStats) = {
    new ReplicaAlterLogDirsManager(config, this, quotaManager, brokerTopicStats)
  }

  protected def createReplicaSelector(): Option[ReplicaSelector] = {
    config.replicaSelectorClassName.map { className =>
      val tmpReplicaSelector: ReplicaSelector = CoreUtils.createObject[ReplicaSelector](className)
      tmpReplicaSelector.configure(config.originals())
      tmpReplicaSelector
    }
  }

  def lastOffsetForLeaderEpoch(
    requestedEpochInfo: Seq[OffsetForLeaderTopic]
  ): Seq[OffsetForLeaderTopicResult] = {
    requestedEpochInfo.map { offsetForLeaderTopic =>
      val partitions = offsetForLeaderTopic.partitions.asScala.map { offsetForLeaderPartition =>
        val tp = new TopicPartition(offsetForLeaderTopic.topic, offsetForLeaderPartition.partition)
        getPartition(tp) match {
          case HostedPartitionRaft.Online(partition) =>
            val currentLeaderEpochOpt =
              if (offsetForLeaderPartition.currentLeaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH)
                Optional.empty[Integer]
              else
                Optional.of[Integer](offsetForLeaderPartition.currentLeaderEpoch)

            partition.lastOffsetForLeaderEpoch(
              currentLeaderEpochOpt,
              offsetForLeaderPartition.leaderEpoch,
              fetchOnlyFromLeader = true)

          case HostedPartitionRaft.Offline | HostedPartitionRaft.Deferred(_, _, _, _, _) =>
            new EpochEndOffset()
              .setPartition(offsetForLeaderPartition.partition)
              .setErrorCode(Errors.KAFKA_STORAGE_ERROR.code)

          case HostedPartitionRaft.None if metadataCache.contains(tp) =>
            new EpochEndOffset()
              .setPartition(offsetForLeaderPartition.partition)
              .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code)

          case HostedPartitionRaft.None =>
            new EpochEndOffset()
              .setPartition(offsetForLeaderPartition.partition)
              .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
        }
      }

      new OffsetForLeaderTopicResult()
        .setTopic(offsetForLeaderTopic.topic)
        .setPartitions(partitions.toList.asJava)
    }
  }

  def electLeaders(
    controller: KafkaController,
    partitions: Set[TopicPartition],
    electionType: ElectionType,
    responseCallback: Map[TopicPartition, ApiError] => Unit,
    requestTimeout: Int
  ): Unit = {

    val deadline = time.milliseconds() + requestTimeout

    def electionCallback(results: Map[TopicPartition, Either[ApiError, Int]]): Unit = {
      val expectedLeaders = mutable.Map.empty[TopicPartition, Int]
      val failures = mutable.Map.empty[TopicPartition, ApiError]
      results.foreach {
        case (partition, Right(leader)) => expectedLeaders += partition -> leader
        case (partition, Left(error)) => failures += partition -> error
      }

      if (expectedLeaders.nonEmpty) {
        val watchKeys = expectedLeaders.iterator.map {
          case (tp, _) => TopicPartitionOperationKey(tp)
        }.toBuffer

        delayedElectLeaderPurgatory.tryCompleteElseWatch(
          new DelayedElectLeader(
            math.max(0, deadline - time.milliseconds()),
            expectedLeaders,
            failures,
            this,
            responseCallback
          ),
          watchKeys
        )
      } else {
          // There are no partitions actually being elected, so return immediately
          responseCallback(failures)
      }
    }

    controller.electLeaders(partitions, electionType, electionCallback)
  }

  def lowWatermarkReached(topicPartition: TopicPartition, requiredOffset: Long): (Boolean, Errors, Long) = {
    getPartition(topicPartition) match {
      case HostedPartitionRaft.Online(partition) =>
        partition.leaderLogIfLocal match {
          case Some(_) =>
            val leaderLW = partition.lowWatermarkIfLeader
            (leaderLW >= requiredOffset, Errors.NONE, leaderLW)
          case None =>
            (false, Errors.NOT_LEADER_OR_FOLLOWER, DeleteRecordsResponse.INVALID_LOW_WATERMARK)
        }

      case HostedPartitionRaft.Deferred(_, _, _, _, _) =>
        (false, Errors.UNKNOWN_TOPIC_OR_PARTITION, DeleteRecordsResponse.INVALID_LOW_WATERMARK)

      case HostedPartitionRaft.Offline =>
        (false, Errors.KAFKA_STORAGE_ERROR, DeleteRecordsResponse.INVALID_LOW_WATERMARK)

      case HostedPartitionRaft.None =>
        (false, Errors.UNKNOWN_TOPIC_OR_PARTITION, DeleteRecordsResponse.INVALID_LOW_WATERMARK)
    }
  }
}
