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
import org.apache.kafka.common.Uuid


case class MetadataPartition(topicName: String,
                             partitionIndex: Int,
                             topicUuid: Uuid = null,
                             leaderId: Int = -1,
                             leaderEpoch: Int = -1,
                             replicas: Seq[Int] = Seq(),
                             isr: Seq[Int] = Seq(),
                             offlineReplicas: Seq[Int] = Seq()) {
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
  var nameList = partitions.nameList.clone().
      asInstanceOf[util.ArrayList[MetadataPartition]]
  var idList = partitions.idList.clone().
    asInstanceOf[util.ArrayList[MetadataPartition]]
  val newNameListEntries = new util.ArrayList[MetadataPartition]
  val newIdListEntries = new util.ArrayList[MetadataPartition]

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

case class MetadataPartitions(nameList: util.ArrayList[MetadataPartition],
                              idList: util.ArrayList[MetadataPartition]) {
  def iterator(): MetadataPartitionsIterator = MetadataPartitionsIterator(nameList, 0)

  def iterator(topicName: String): MetadataPartitionsIterator = {
    val exemplar = MetadataPartition(topicName, 0)
    val result = Collections.binarySearch(nameList, exemplar, NAME_COMPARATOR)
    if (result < 0) {
      MetadataPartitionsIterator(nameList, nameList.size())
    } else {
      MetadataPartitionsIterator(nameList, result)
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

case class MetadataPartitionsIterator(partitionList: util.ArrayList[MetadataPartition],
                                      var index: Int) extends util.Iterator[MetadataPartition] {
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
