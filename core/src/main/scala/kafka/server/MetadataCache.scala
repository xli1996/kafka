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

import java.util
import java.util.Collections

import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.{Cluster, Node, PartitionInfo, TopicPartition}

import scala.jdk.CollectionConverters._

case class MetadataCacheBuilder(prevCache: MetadataCache) {
  var _partitionsBuilder: MetadataPartitionsBuilder = null
  var _controllerId = prevCache.controllerId
  var _brokersBuilder: MetadataBrokersBuilder = null

  def partitionsBuilder(): MetadataPartitionsBuilder = {
    if (partitionsBuilder == null) {
      _partitionsBuilder = new MetadataPartitionsBuilder(prevCache.partitions)
    }
    _partitionsBuilder
  }

  def setControllerId(controllerId: Option[Int]) = {
    _controllerId = controllerId
  }

  def brokersBuilder(): MetadataBrokersBuilder = {
    if (partitionsBuilder == null) {
      _brokersBuilder = new MetadataBrokersBuilder(prevCache.brokers)
    }
    _brokersBuilder
  }

  def build(): MetadataCache = {
    val nextPartitions = if (_partitionsBuilder == null) {
      prevCache.partitions
    } else {
      _partitionsBuilder.build()
    }
    val nextBrokers = if (_brokersBuilder == null) {
      prevCache.brokers
    } else {
      _brokersBuilder.build()
    }
    MetadataCache(nextPartitions, _controllerId, nextBrokers)
  }
}

case class MetadataCache(partitions: MetadataPartitions,
                         controllerId: Option[Int],
                         brokers: MetadataBrokers) {

  def contains(partition: TopicPartition): Boolean =
    partitions.get(partition.topic(), partition.partition()).isDefined

  def contains(topic: String): Boolean = partitions.iterator(topic).hasNext

  def getAliveBroker(id: Int): Option[MetadataBroker] = brokers.get(id)

  def numAliveBrokers(): Int = {
    brokers.iterator().asScala.count(!_.fenced)
  }

  // if the leader is not known, return None;
  // if the leader is known and corresponding node is available, return Some(node)
  // if the leader is known but corresponding node with the listener name is not available, return Some(NO_NODE)
  def getPartitionLeaderEndpoint(topic: String, partitionId: Int, listenerName: ListenerName): Option[Node] = {
    partitions.get(topic, partitionId) match {
      case None => None
      case Some(partition) =>
        val leaderId = partition.leaderId
        if (leaderId < 0) {
          None
        } else {
          brokers.getAlive(leaderId).flatMap(broker => broker.endPoints.get(listenerName.value()))
        }
    }
  }

  def controller(): Option[MetadataBroker] = controllerId.flatMap(id => brokers.getAlive(id))

  def getClusterMetadata(clusterId: String, listenerName: ListenerName): Cluster = {
    val nodes = new util.HashMap[Integer, Node]
    brokers.iterator().asScala.foreach {
      case node => if (!node.fenced) {
        node.endPoints.get(listenerName.value()).foreach { nodes.put(node.id, _) }
      }
    }

    def node(id: Integer): Node = {
      Some(nodes.get(id)).getOrElse(Node.noNode())
    }

    val partitionInfos = new util.ArrayList[PartitionInfo]
    val internalTopics = new util.HashSet[String]

    partitions.iterator().asScala.foreach {
      case partition =>
        partitionInfos.add(new PartitionInfo(partition.topicName,
          partition.partitionIndex, node(partition.leaderId),
          partition.replicas.map(node(_)).toArray,
          partition.isr.map(node(_)).toArray,
          partition.offlineReplicas.map(node(_)).toArray))
        if (Topic.isInternal(partition.topicName)) {
          internalTopics.add(partition.topicName)
        }
    }

    val unauthorizedTopics = Collections.emptySet[String]

    new Cluster(clusterId, nodes.values(),
      partitionInfos, unauthorizedTopics, internalTopics,
      controller().getOrElse(Node.noNode()))
  }
}

