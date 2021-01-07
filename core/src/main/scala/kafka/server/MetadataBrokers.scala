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

import org.apache.kafka.common.Node

case class MetadataBroker(id: Int,
                          endPoints: collection.Map[String, Node] = Map(),
                          fenced: Boolean = false,
                          epoch: Long = -1L) {
}

class MetadataBrokerComparator extends Comparator[MetadataBroker] with Serializable {
  override def compare(a: MetadataBroker, b: MetadataBroker): Int = Integer.compare(a.id, b.id)
}

object MetadataBrokers {
  val COMPARATOR = new MetadataBrokerComparator()
}

class MetadataBrokersBuilder(brokers: MetadataBrokers) {
  var brokerList = brokers.brokerList.clone().
      asInstanceOf[util.ArrayList[MetadataBroker]]

  val newBrokersList = new util.ArrayList[MetadataBroker]

  def add(broker: MetadataBroker): Unit = {
    val result = Collections.binarySearch(brokerList, broker, MetadataBrokers.COMPARATOR)
    if (result >= 0) {
      brokerList.set(result, broker)
    } else {
      newBrokersList.add(broker)
    }
  }

  def build(): MetadataBrokers = {
    if (!newBrokersList.isEmpty()) {
      brokerList.addAll(newBrokersList)
      brokerList.sort(MetadataBrokers.COMPARATOR)
    }
    val result = MetadataBrokers(brokerList)
    brokerList = null
    result
  }
}

case class MetadataBrokers(brokerList: util.ArrayList[MetadataBroker]) {
  def iterator(): MetadataBrokersIterator = MetadataBrokersIterator(brokerList, 0)

  def get(id: Int): Option[MetadataBroker] = {
    val exemplar = new MetadataBroker(id)
    val result = Collections.binarySearch(brokerList, exemplar, MetadataBrokers.COMPARATOR)
    if (result < 0) {
      None
    } else {
      Some(brokerList.get(result))
    }
  }
}

case class MetadataBrokersIterator(partitionList: util.ArrayList[MetadataBroker],
                                   var index: Int) extends util.Iterator[MetadataBroker] {
  override def hasNext: Boolean = index < partitionList.size()

  override def next(): MetadataBroker = {
    if (!hasNext()) {
      throw new NoSuchElementException()
    }
    val result = partitionList.get(index)
    index = index + 1
    result
  }
}
