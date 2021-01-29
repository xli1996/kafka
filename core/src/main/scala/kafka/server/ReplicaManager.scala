package kafka.server

import java.io.File
import java.util.Optional
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.Lock

import com.yammer.metrics.core.Meter
import kafka.cluster.{BrokerEndPoint, Partition}
import kafka.controller.KafkaController
import kafka.log.{AppendOrigin, Log, LogAppendInfo, LogConfig, LogManager}
import kafka.metrics.KafkaMetricsGroup
import kafka.server.checkpoints.{OffsetCheckpointFile, OffsetCheckpoints}
import kafka.server.metadata.ConfigRepository
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResult
import org.apache.kafka.common.message.{DescribeLogDirsResponseData, FetchResponseData}
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopic
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult
import org.apache.kafka.common.{ElectionType, IsolationLevel, TopicPartition}
import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaPartitionState
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record.{MemoryRecords, RecordConversionStats, Records}
import org.apache.kafka.common.replica.ClientMetadata
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.requests.FetchResponse.AbortedTransaction
import org.apache.kafka.common.requests.{ApiError, DescribeLogDirsResponse, LeaderAndIsrRequest, LeaderAndIsrResponse, UpdateMetadataRequest}
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse

import scala.jdk.CollectionConverters._
import scala.collection.{Map, Seq, Set, mutable}

/*
 * Result metadata of a log append operation on the log
 */
case class LogAppendResult(info: LogAppendInfo, exception: Option[Throwable] = None) {
  def error: Errors = exception match {
    case None => Errors.NONE
    case Some(e) => Errors.forException(e)
  }
}

case class LogDeleteRecordsResult(requestedOffset: Long, lowWatermark: Long, exception: Option[Throwable] = None) {
  def error: Errors = exception match {
    case None => Errors.NONE
    case Some(e) => Errors.forException(e)
  }
}

/**
 * Result metadata of a log read operation on the log
 * @param info @FetchDataInfo returned by the @Log read
 * @param divergingEpoch Optional epoch and end offset which indicates the largest epoch such
 *                       that subsequent records are known to diverge on the follower/consumer
 * @param highWatermark high watermark of the local replica
 * @param leaderLogStartOffset The log start offset of the leader at the time of the read
 * @param leaderLogEndOffset The log end offset of the leader at the time of the read
 * @param followerLogStartOffset The log start offset of the follower taken from the Fetch request
 * @param fetchTimeMs The time the fetch was received
 * @param lastStableOffset Current LSO or None if the result has an exception
 * @param preferredReadReplica the preferred read replica to be used for future fetches
 * @param exception Exception if error encountered while reading from the log
 */
case class LogReadResult(info: FetchDataInfo,
                         divergingEpoch: Option[FetchResponseData.EpochEndOffset],
                         highWatermark: Long,
                         leaderLogStartOffset: Long,
                         leaderLogEndOffset: Long,
                         followerLogStartOffset: Long,
                         fetchTimeMs: Long,
                         lastStableOffset: Option[Long],
                         preferredReadReplica: Option[Int] = None,
                         exception: Option[Throwable] = None) {

  def error: Errors = exception match {
    case None => Errors.NONE
    case Some(e) => Errors.forException(e)
  }

  def withEmptyFetchInfo: LogReadResult =
    copy(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY))

  override def toString = {
    "LogReadResult(" +
      s"info=$info, " +
      s"divergingEpoch=$divergingEpoch, " +
      s"highWatermark=$highWatermark, " +
      s"leaderLogStartOffset=$leaderLogStartOffset, " +
      s"leaderLogEndOffset=$leaderLogEndOffset, " +
      s"followerLogStartOffset=$followerLogStartOffset, " +
      s"fetchTimeMs=$fetchTimeMs, " +
      s"preferredReadReplica=$preferredReadReplica, " +
      s"lastStableOffset=$lastStableOffset, " +
      s"error=$error" +
      ")"
  }

}

case class FetchPartitionData(error: Errors = Errors.NONE,
                              highWatermark: Long,
                              logStartOffset: Long,
                              records: Records,
                              divergingEpoch: Option[FetchResponseData.EpochEndOffset],
                              lastStableOffset: Option[Long],
                              abortedTransactions: Option[List[AbortedTransaction]],
                              preferredReadReplica: Option[Int],
                              isReassignmentFetch: Boolean)

case class IsrChangePropagationConfig(
  // How often to check for ISR
  checkIntervalMs: Long,

  // Maximum time that an ISR change may be delayed before sending the notification
  maxDelayMs: Long,

  // Maximum time to await additional changes before sending the notification
  lingerMs: Long
)

object ReplicaManager {
  val HighWatermarkFilename = "replication-offset-checkpoint"

  // This field is mutable to allow overriding change notification behavior in test cases
  @volatile var DefaultIsrPropagationConfig: IsrChangePropagationConfig = IsrChangePropagationConfig(
    checkIntervalMs = 2500,
    lingerMs = 5000,
    maxDelayMs = 60000,
  )
}

trait ReplicaManager extends KafkaMetricsGroup {
  val config: KafkaConfig
  val zkClient: Option[KafkaZkClient] // must be supplied with IBP < KAFKA_2_7_IV2
  val logManager: LogManager
  val isShuttingDown: AtomicBoolean
  val brokerTopicStats: BrokerTopicStats
  val metadataCache: MetadataCache
  val delayedProducePurgatory: DelayedOperationPurgatory[DelayedProduce]
  val delayedFetchPurgatory: DelayedOperationPurgatory[DelayedFetch]
  val delayedDeleteRecordsPurgatory: DelayedOperationPurgatory[DelayedDeleteRecords]
  val delayedElectLeaderPurgatory: DelayedOperationPurgatory[DelayedElectLeader]
  val configRepository: ConfigRepository
  val alterIsrManager: AlterIsrManager
  val replicaFetcherManager: ReplicaFetcherManager
  val replicaAlterLogDirsManager: ReplicaAlterLogDirsManager
  val helper: ReplicaManagerHelper

  newGauge("LeaderCount", () => leaderPartitionsIterator.size)
  newGauge("OfflineReplicaCount", () => offlinePartitionCount)
  newGauge("UnderReplicatedPartitions", () => underReplicatedPartitionCount)
  newGauge("UnderMinIsrPartitionCount", () => leaderPartitionsIterator.count(_.isUnderMinIsr))
  newGauge("AtMinIsrPartitionCount", () => leaderPartitionsIterator.count(_.isAtMinIsr))
  newGauge("ReassigningPartitions", () => reassigningPartitionsCount)
  val isrExpandRate: Meter = newMeter("IsrExpandsPerSec", "expands", TimeUnit.SECONDS)
  val isrShrinkRate: Meter = newMeter("IsrShrinksPerSec", "shrinks", TimeUnit.SECONDS)
  val failedIsrUpdatesRate: Meter = newMeter("FailedIsrUpdatesPerSec", "failedUpdates", TimeUnit.SECONDS)

  def leaderPartitionsIterator: Iterator[Partition] = {
    onlinePartitionsIterator.filter(_.leaderLogIfLocal.isDefined)
  }

  // An iterator over all online partitions. This is a weakly consistent iterator; a partition made
  // offline after the iterator has been constructed could still be returned by this iterator.
  def onlinePartitionsIterator: Iterator[Partition]
  def offlinePartitionCount: Int
  def underReplicatedPartitionCount: Int = leaderPartitionsIterator.count(_.isUnderReplicated)
  def reassigningPartitionsCount: Int = leaderPartitionsIterator.count(_.isReassigning)

  def allPartitionsCount: Int
  def recordIsrChange(topicPartition: TopicPartition): Unit
  def tryCompleteActions(): Unit
  def getMagic(topicPartition: TopicPartition): Option[Byte] = getLogConfig(topicPartition).map(_.messageFormatVersion.recordVersion.value)
  def onlinePartition(topicPartition: TopicPartition): Option[Partition]
  def onlinePartitionOrException(topicPartition: TopicPartition): Partition
  def onlinePartitionOrError(topicPartition: TopicPartition): Either[Errors, Partition]
  def futureLocalOnlineLogOrException(topicPartition: TopicPartition): Log = {
    onlinePartitionOrException(topicPartition).futureLocalLogOrException
  }
  def futureLocalOnlineLogExists(topicPartition: TopicPartition): Boolean = {
    onlinePartitionOrException(topicPartition).futureLog.isDefined
  }

  def localOnlineLogOrException(topicPartition: TopicPartition): Log = {
    onlinePartitionOrException(topicPartition).localLogOrException
  }
  // Returns the Log for the partition if it is online.
  // Equivalent to futureLocalOnlineLogOrException() when using ZooKeeper.
  def localOnlineLog(topicPartition: TopicPartition): Option[Log] = {
    onlinePartition(topicPartition).flatMap(_.log)
  }

  def becomeLeaderOrFollower(correlationId: Int,
                             leaderAndIsrRequest: LeaderAndIsrRequest,
                             onLeadershipChange: (Iterable[Partition], Iterable[Partition]) => Unit): LeaderAndIsrResponse = {
    throw new UnsupportedOperationException() // gets overridden in ZooKeeper version
  }

  def stopReplicas(correlationId: Int,
                   controllerId: Int,
                   controllerEpoch: Int,
                   brokerEpoch: Long,
                   partitionStates: Map[TopicPartition, StopReplicaPartitionState]
                  ): (mutable.Map[TopicPartition, Errors], Errors) = {
    throw new UnsupportedOperationException() // gets overridden in ZooKeeper version
  }

  def maybeUpdateMetadataCache(correlationId: Int, updateMetadataRequest: UpdateMetadataRequest) : Seq[TopicPartition] = {
    throw new UnsupportedOperationException() // gets overridden in ZooKeeper version
  }

  def hasDelayedElectionOperations: Boolean = delayedElectLeaderPurgatory.numDelayed != 0

  def tryCompleteElection(key: DelayedOperationKey): Unit = {
    helper.tryCompleteElection(this, key)
  }

  def getLogConfig(topicPartition: TopicPartition): Option[LogConfig] = localOnlineLog(topicPartition).map(_.config)

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
                    recordConversionStatsCallback: Map[TopicPartition, RecordConversionStats] => Unit = _ => ()): Unit

  /**
   *  To avoid ISR thrashing, we only throttle a replica on the leader if it's in the throttled replica list,
   *  the quota is exceeded and the replica is not in sync.
   */
  def shouldLeaderThrottle(quota: ReplicaQuota, partition: Partition, replicaId: Int): Boolean

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
                       clientMetadata: Option[ClientMetadata]): Seq[(TopicPartition, LogReadResult)]

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
                    clientMetadata: Option[ClientMetadata]): Unit

  def getLogEndOffset(topicPartition: TopicPartition): Option[Long] =
    onlinePartition(topicPartition).flatMap(_.leaderLogIfLocal.map(_.logEndOffset))
  def getLogEndOffsetLag(topicPartition: TopicPartition, logEndOffset: Long, isFuture: Boolean): Long = {
    helper.getLogEndOffsetLag(this, topicPartition, logEndOffset, isFuture)
  }
  def getLog(topicPartition: TopicPartition): Option[Log]

  def isAddingReplica(topicPartition: TopicPartition, replicaId: Int): Boolean

  def legacyFetchOffsetsForTimestamp(topicPartition: TopicPartition,
                                     timestamp: Long,
                                     maxNumOffsets: Int,
                                     isFromConsumer: Boolean,
                                     fetchOnlyFromLeader: Boolean): Seq[Long]

  def fetchOffsetForTimestamp(topicPartition: TopicPartition,
                              timestamp: Long,
                              isolationLevel: Option[IsolationLevel],
                              currentLeaderEpoch: Optional[Integer],
                              fetchOnlyFromLeader: Boolean): Option[TimestampAndOffset]

  def lastOffsetForLeaderEpoch(
                                requestedEpochInfo: Seq[OffsetForLeaderTopic]
                              ): Seq[OffsetForLeaderTopicResult]

  /**
   * For each pair of partition and log directory specified in the map, if the partition has already been created on
   * this broker, move its log files to the specified log directory. Otherwise, record the pair in the memory so that
   * the partition will be created in the specified log directory when broker receives LeaderAndIsrRequest for the partition later.
   */
  def alterReplicaLogDirs(partitionDirs: Map[TopicPartition, String]): Map[TopicPartition, Errors]

  def deleteRecords(timeout: Long,
                    offsetPerPartition: Map[TopicPartition, Long],
                    responseCallback: Map[TopicPartition, DeleteRecordsPartitionResult] => Unit): Unit

  /*
   * Get the LogDirInfo for the specified list of partitions.
   *
   * Each LogDirInfo specifies the following information for a given log directory:
   * 1) Error of the log directory, e.g. whether the log is online or offline
   * 2) size and lag of current and future logs for each partition in the given log directory. Only logs of the queried partitions
   *    are included. There may be future logs (which will replace the current logs of the partition in the future) on the broker after KIP-113 is implemented.
   */
  def describeLogDirs(partitions: Set[TopicPartition]): List[DescribeLogDirsResponseData.DescribeLogDirsResult] = {
    helper.describeLogDirs(this, partitions)
  }

  def electLeaders(
                    controller: KafkaController,
                    partitions: Set[TopicPartition],
                    electionType: ElectionType,
                    responseCallback: Map[TopicPartition, ApiError] => Unit,
                    requestTimeout: Int
                  ): Unit

  def lowWatermarkReached(topicPartition: TopicPartition, requiredOffset: Long): (Boolean, Errors, Long)

  def startup(): Unit

  // High watermark do not need to be checkpointed only when under unit tests
  def shutdown(checkpointHW: Boolean = true): Unit
}

class ReplicaManagerHelper() {

  def tryCompleteElection(replicaManager: ReplicaManager, key: DelayedOperationKey): Unit = {
    val completed = replicaManager.delayedElectLeaderPurgatory.checkAndComplete(key)
    replicaManager.debug("Request key %s unblocked %d ElectLeader.".format(key.keyLabel, completed))
  }

  def getLogEndOffsetLag(replicaManager: ReplicaManager,
                         topicPartition: TopicPartition, logEndOffset: Long, isFuture: Boolean): Long = {
    replicaManager.localOnlineLog(topicPartition) match {
      case Some(log) =>
        if (isFuture)
          log.logEndOffset - logEndOffset
        else
          math.max(log.highWatermark - logEndOffset, 0)
      case None =>
        // return -1L to indicate that the LEO lag is not available if the replica is not created or is offline
        DescribeLogDirsResponse.INVALID_OFFSET_LAG
    }
  }

  /*
   * Get the LogDirInfo for the specified list of partitions.
   *
   * Each LogDirInfo specifies the following information for a given log directory:
   * 1) Error of the log directory, e.g. whether the log is online or offline
   * 2) size and lag of current and future logs for each partition in the given log directory. Only logs of the queried partitions
   *    are included. There may be future logs (which will replace the current logs of the partition in the future) on the broker after KIP-113 is implemented.
   */
  def describeLogDirs(replicaManager: ReplicaManager, partitions: Set[TopicPartition]): List[DescribeLogDirsResponseData.DescribeLogDirsResult] = {
    val config = replicaManager.config
    val logManager = replicaManager.logManager

    val logsByDir = logManager.allLogs.groupBy(log => log.parentDir)

    config.logDirs.toSet.map { logDir: String =>
      val absolutePath = new File(logDir).getAbsolutePath
      try {
        if (!logManager.isLogDirOnline(absolutePath))
          throw new KafkaStorageException(s"Log directory $absolutePath is offline")

        logsByDir.get(absolutePath) match {
          case Some(logs) =>
            val topicInfos = logs.groupBy(_.topicPartition.topic).map{case (topic, logs) =>
              new DescribeLogDirsResponseData.DescribeLogDirsTopic().setName(topic).setPartitions(
                logs.filter { log =>
                  partitions.contains(log.topicPartition)
                }.map { log =>
                  new DescribeLogDirsResponseData.DescribeLogDirsPartition()
                    .setPartitionSize(log.size)
                    .setPartitionIndex(log.topicPartition.partition)
                    .setOffsetLag(replicaManager.getLogEndOffsetLag(log.topicPartition, log.logEndOffset, log.isFuture))
                    .setIsFutureKey(log.isFuture)
                }.toList.asJava)
            }.toList.asJava

            new DescribeLogDirsResponseData.DescribeLogDirsResult().setLogDir(absolutePath)
              .setErrorCode(Errors.NONE.code).setTopics(topicInfos)
          case None =>
            new DescribeLogDirsResponseData.DescribeLogDirsResult().setLogDir(absolutePath)
              .setErrorCode(Errors.NONE.code)
        }

      } catch {
        case e: KafkaStorageException =>
          replicaManager.warn("Unable to describe replica dirs for %s".format(absolutePath), e)
          new DescribeLogDirsResponseData.DescribeLogDirsResult()
            .setLogDir(absolutePath)
            .setErrorCode(Errors.KAFKA_STORAGE_ERROR.code)
        case t: Throwable =>
          replicaManager.error(s"Error while describing replica in dir $absolutePath", t)
          new DescribeLogDirsResponseData.DescribeLogDirsResult()
            .setLogDir(absolutePath)
            .setErrorCode(Errors.forException(t).code)
      }
    }.toList
  }

  // Flushes the highwatermark value for all partitions to the highwatermark file
  def checkpointHighWatermarks(replicaManager: ReplicaManager,
                               highWatermarkCheckpoints: () => Map[String, OffsetCheckpointFile]): Unit = {
    def putHw(logDirToCheckpoints: mutable.AnyRefMap[String, mutable.AnyRefMap[TopicPartition, Long]],
              log: Log): Unit = {
      val checkpoints = logDirToCheckpoints.getOrElseUpdate(log.parentDir,
        new mutable.AnyRefMap[TopicPartition, Long]())
      checkpoints.put(log.topicPartition, log.highWatermark)
    }

    val logDirToHws = new mutable.AnyRefMap[String, mutable.AnyRefMap[TopicPartition, Long]](
      replicaManager.allPartitionsCount)
    replicaManager.onlinePartitionsIterator.foreach { partition =>
      partition.log.foreach(putHw(logDirToHws, _))
      partition.futureLog.foreach(putHw(logDirToHws, _))
    }

    for ((logDir, hws) <- logDirToHws) {
      try highWatermarkCheckpoints().get(logDir).foreach(_.write(hws))
      catch {
        case e: KafkaStorageException =>
          replicaManager.error(s"Error while writing to highwatermark file in directory $logDir", e)
      }
    }
  }

  /**
   * Update the leader and follower metrics.
   *
   * For topic partitions of which the broker is no longer a leader, delete metrics related to
   * those topics. Note that this means the broker stops being either a replica or a leader of
   * partitions of said topics.  See KAFKA-8392 for more information.
   */
  def updateLeaderAndFollowerMetrics(replicaManager: ReplicaManager,
                                     newFollowerTopics: Set[String]): Unit = {
    val leaderTopicSet = replicaManager.leaderPartitionsIterator.map(_.topic).toSet
    newFollowerTopics.diff(leaderTopicSet).foreach(replicaManager.brokerTopicStats.removeOldLeaderMetrics)

    // remove metrics for brokers which are not followers of a topic
    leaderTopicSet.diff(newFollowerTopics).foreach(replicaManager.brokerTopicStats.removeOldFollowerMetrics)
  }

  def maybeAddLogDirFetchers(replicaManager: ReplicaManager,
                             partitions: Set[Partition],
                             offsetCheckpoints: OffsetCheckpoints): Unit = {
    val logManager = replicaManager.logManager
    val config = replicaManager.config
    val futureReplicasAndInitialOffset = new mutable.HashMap[TopicPartition, InitialFetchState]
    for (partition <- partitions) {
      val topicPartition = partition.topicPartition
      if (logManager.getLog(topicPartition, isFuture = true).isDefined) {
        partition.log.foreach { log =>
          val leader = BrokerEndPoint(config.brokerId, "localhost", -1)

          // Add future replica log to partition's map
          partition.createLogIfNotExists(
            isNew = false,
            isFutureReplica = true,
            offsetCheckpoints)

          // pause cleaning for partitions that are being moved and start ReplicaAlterDirThread to move
          // replica from source dir to destination dir
          logManager.abortAndPauseCleaning(topicPartition)

          futureReplicasAndInitialOffset.put(topicPartition, InitialFetchState(leader,
            partition.getLeaderEpoch, log.highWatermark))
        }
      }
    }

    if (futureReplicasAndInitialOffset.nonEmpty)
      replicaManager.replicaAlterLogDirsManager.addFetcherForPartitions(futureReplicasAndInitialOffset)
  }

  def completeDelayedFetchOrProduceRequests(replicaManager: ReplicaManager,
                                            topicPartition: TopicPartition): Unit = {
    val topicPartitionOperationKey = TopicPartitionOperationKey(topicPartition)
    replicaManager.delayedProducePurgatory.checkAndComplete(topicPartitionOperationKey)
    replicaManager.delayedFetchPurgatory.checkAndComplete(topicPartitionOperationKey)
  }
}