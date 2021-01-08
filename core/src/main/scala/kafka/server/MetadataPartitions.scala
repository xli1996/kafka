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
import java.util.function.BiConsumer

import org.apache.kafka.common.{TopicPartition, Uuid}


case class MetadataPartition(topicName: String,
                             partitionIndex: Int,
                             topicUuid: Uuid,
                             leaderId: Int,
                             leaderEpoch: Int,
                             replicas: util.List[Integer],
                             isr: util.List[Integer],
                             offlineReplicas: Seq[Int]) {
  def toTopicPartition(): TopicPartition = {
    new TopicPartition(topicName, partitionIndex)
  }
}

class MetadataPartitionsBuilder(prevPartitions: MetadataPartitions) {
  private var newNameMap = prevPartitions.copyNameMap()
  private var newIdMap = prevPartitions.copyIdMap()
  private val changed = new util.IdentityHashMap[Any, Boolean]()

  def set(partition: MetadataPartition): Unit = {
    val prevNameMapping = newIdMap.get(partition.topicUuid)
    if (prevNameMapping == null) {
      newIdMap.put(partition.topicUuid, partition.topicName)
    } else if (!prevNameMapping.equals(partition.topicName)) {
      throw new RuntimeException("Inconsistent topic ID mappings: topic " +
        s"${partition.topicUuid} was formerly known as ${prevNameMapping}, but now is " +
        s"known as ${partition.topicName}")
    }
    val prevPartitionMap = newNameMap.get(partition.topicName)
    val newPartitionMap = if (prevPartitionMap == null) {
      val m = new util.HashMap[Int, MetadataPartition](1)
      changed.put(m, true)
      m
    } else if (changed.containsKey(prevPartitionMap)) {
      prevPartitionMap
    } else {
      val m = new util.HashMap[Int, MetadataPartition](prevPartitionMap.size() + 1)
      m.putAll(prevPartitionMap)
      changed.put(m, true)
      m
    }
    newPartitionMap.put(partition.partitionIndex, partition)
    newNameMap.put(partition.topicName, newPartitionMap)
  }

  def remove(topicName: String, partitionId: Int): Unit = {
    val prevPartitionMap = newNameMap.get(topicName)
    if (prevPartitionMap != null) {
      if (changed.containsKey(prevPartitionMap)) {
        prevPartitionMap.remove(partitionId)
      } else {
        val newPartitionMap = new util.HashMap[Int, MetadataPartition](prevPartitionMap.size() - 1)
        prevPartitionMap.forEach(new BiConsumer[Int, MetadataPartition]() {
          override def accept(key: Int, value: MetadataPartition): Unit =
            if (!key.equals(partitionId)) {
              newPartitionMap.put(key, value)
            }
        })
        changed.put(newPartitionMap, true)
        newNameMap.put(topicName, newPartitionMap)
      }
    }
  }

  def build(): MetadataPartitions = {
    val result = new MetadataPartitions(newNameMap, newIdMap)
    newNameMap = null
    newIdMap = null
    result
  }
}

class MetadataPartitions(private val nameMap: util.Map[String, util.Map[Int, MetadataPartition]],
                         private val idMap: util.Map[Uuid, String]) {

  def copyNameMap(): util.Map[String, util.Map[Int, MetadataPartition]] = {
    val copy = new util.HashMap[String, util.Map[Int, MetadataPartition]]
    val iterator = nameMap.entrySet().iterator()
    while (iterator.hasNext) {
      val entry = iterator.next()
      copy.put(entry.getKey, entry.getValue)
    }
    copy
  }

  def copyIdMap(): util.Map[Uuid, String] = {
    val copy = new util.HashMap[Uuid, String]
    val iterator = idMap.entrySet().iterator()
    while (iterator.hasNext) {
      val entry = iterator.next()
      copy.put(entry.getKey, entry.getValue)
    }
    copy
  }

  def allPartitions(): util.Iterator[MetadataPartition] = new AllPartitionsIterator(nameMap)

  def topicPartitions(topicName: String): util.Iterator[MetadataPartition] = {
    val partitionMap = nameMap.get(topicName)
    if (partitionMap == null) {
      Collections.emptyIterator()
    } else {
      partitionMap.values().iterator()
    }
  }

  def get(topicName: String, partitionId: Int): Option[MetadataPartition] = {
    Option(nameMap.get(topicName)).flatMap(_.get(partitionId))
  }
}

class AllPartitionsIterator(nameMap: util.Map[String, util.Map[Int, MetadataPartition]])
    extends util.Iterator[MetadataPartition] {

  val outerIterator = nameMap.values().iterator()

  var innerIterator: util.Iterator[MetadataPartition] = Collections.emptyIterator()

  var _next: MetadataPartition = null

  override def hasNext: Boolean = {
    if (_next != null) {
      true
    } else {
      while (!innerIterator.hasNext) {
        if (!outerIterator.hasNext) {
          return false
        } else {
          innerIterator = outerIterator.next().values().iterator()
        }
      }
      _next = innerIterator.next()
      true
    }
  }

  override def next(): MetadataPartition = {
    if (!hasNext()) {
      throw new NoSuchElementException()
    }
    val result = _next
    _next = null
    result
  }
}
