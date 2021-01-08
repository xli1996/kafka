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

import java.io.Serializable
import java.util
import java.util.{Collections, Comparator}

import kafka.server.MetadataPartitions.NAME_COMPARATOR
import org.apache.kafka.common.{TopicPartition, Uuid}


case class MetadataPartition(topicName: String,
                             partitionIndex: Int,
                             topicUuid: Uuid = null,
                             leaderId: Int = -1,
                             leaderEpoch: Int = -1,
                             replicas: Seq[Int] = Seq(),
                             isr: Seq[Int] = Seq(),
                             offlineReplicas: Seq[Int] = Seq()) {
  def toTopicPartition(): TopicPartition = {
    new TopicPartition(topicName, partitionIndex)
  }
}

class MetadataPartitionTopicNameComparator extends Comparator[MetadataPartition] with Serializable {
  override def compare(a: MetadataPartition, b: MetadataPartition): Int = {
    val topicNameResult = a.topicName.compareTo(b.topicName)
    if (topicNameResult != 0) {
      topicNameResult
    } else {
      Integer.compare(a.partitionIndex, b.partitionIndex)
    }
  }
}

class MetadataPartitionTopicIdComparator extends Comparator[MetadataPartition] with Serializable {
  override def compare(a: MetadataPartition, b: MetadataPartition): Int = {
    val uuidResult = a.topicUuid.compareTo(b.topicUuid)
    if (uuidResult != 0) {
      uuidResult
    } else {
      Integer.compare(a.partitionIndex, b.partitionIndex)
    }
  }
}

object MetadataPartitions {
  val NAME_COMPARATOR = new MetadataPartitionTopicNameComparator()
  val ID_COMPARATOR = new MetadataPartitionTopicIdComparator()
}

class MetadataPartitionsBuilder(partitions: MetadataPartitions) {
  private var nameList = partitions.cloneNameList()
  private var idList = partitions.cloneIdList()
    asInstanceOf[util.ArrayList[MetadataPartition]]
  private val newNameListEntries = new util.ArrayList[MetadataPartition]
  private val newIdListEntries = new util.ArrayList[MetadataPartition]

  def set(partition: MetadataPartition): Unit = {
    val nameResult = Collections.binarySearch(nameList, partition, MetadataPartitions.NAME_COMPARATOR)
    if (nameResult >= 0) {
      nameList.set(nameResult, partition)
    } else {
      newNameListEntries.add(partition)
    }
    val uuidResult = Collections.binarySearch(idList, partition, MetadataPartitions.ID_COMPARATOR)
    if (uuidResult >= 0) {
      idList.set(uuidResult, partition)
    } else {
      newIdListEntries.add(partition)
    }
  }

  def build(): MetadataPartitions = {
    if (!newNameListEntries.isEmpty()) {
      nameList.addAll(newNameListEntries)
      nameList.sort(MetadataPartitions.NAME_COMPARATOR)
    }
    if (!newIdListEntries.isEmpty()) {
      idList.addAll(newIdListEntries)
      idList.sort(MetadataPartitions.ID_COMPARATOR)
    }
    val result = MetadataPartitions(nameList, idList)
    nameList = null
    idList = null
    result
  }
}

class MetadataPartitions(private val nameList: util.ArrayList[MetadataPartition],
                         private val idList: util.ArrayList[MetadataPartition]) {
  def cloneNameList(): util.ArrayList[MetadataPartition] =
    nameList.clone().asInstanceOf[util.ArrayList[MetadataPartition]]

  def cloneIdList(): util.ArrayList[MetadataPartition] =
    idList.clone().asInstanceOf[util.ArrayList[MetadataPartition]]

  def iterator(): PartitionsIterator = new PartitionsIterator(nameList, 0)

  def iterator(topicName: String): PartitionsInTopicIterator = {
    val exemplar = MetadataPartition(topicName, 0)
    val result = Collections.binarySearch(nameList, exemplar, NAME_COMPARATOR)
    if (result < 0) {
      new PartitionsInTopicIterator(nameList, nameList.size(), topicName)
    } else {
      new PartitionsInTopicIterator(nameList, result, topicName)
    }
  }

  def get(topicName: String, partitionId: Int): Option[MetadataPartition] = {
    val exemplar = MetadataPartition(topicName, partitionId)
    val result = Collections.binarySearch(nameList, exemplar, NAME_COMPARATOR)
    if (result < 0) {
      None
    } else {
      Some(nameList.get(result))
    }
  }
}

class PartitionsInTopicIterator(private val partitionList: util.ArrayList[MetadataPartition],
                                private var index: Int,
                                private val topic: String) extends util.Iterator[MetadataPartition] {
  var _next: MetadataPartition = null

  override def hasNext: Boolean = {
    if (_next != null) {
      true
    } else if (index >= partitionList.size()) {
      false
    } else {
      val partition = partitionList.get(index)
      if (!partition.topicName.equals(topic)) {
        false
      } else {
        _next = partition
        index = index + 1
        true
      }
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

class PartitionsIterator(private val partitionList: util.ArrayList[MetadataPartition],
                         private var index: Int) extends util.Iterator[MetadataPartition] {
  override def hasNext: Boolean = index < partitionList.size()

  override def next(): MetadataPartition = {
    if (!hasNext()) {
      throw new NoSuchElementException()
    }
    val result = partitionList.get(index)
    index = index + 1
    result
  }
}