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

import kafka.network.ConnectionQuotas
import kafka.security.CredentialProvider
import kafka.server.ConfigEntityName
import kafka.server.QuotaFactory.QuotaManagers
import kafka.utils.Logging
import org.apache.kafka.common.config.internals.QuotaConfigs
import org.apache.kafka.common.metadata.QuotaRecord
import org.apache.kafka.common.metrics.Quota
import org.apache.kafka.common.quota.ClientQuotaEntity
import org.apache.kafka.common.utils.Sanitizer

import java.net.{InetAddress, UnknownHostException}
import scala.collection.mutable

/**
 * Watch for changes to quotas in the metadata log and update quota managers as necessary
 *
 * @param quotaManagers
 * @param connectionQuotas
 */
class QuotaMetadataProcessor(val quotaManagers: QuotaManagers,
                             val connectionQuotas: ConnectionQuotas) extends BrokerMetadataProcessor with Logging {

  override def process(event: BrokerMetadataEvent): Unit = {
    event match {
      case MetadataLogEvent(apiMessages, _) =>
        apiMessages.forEach {
          case record: QuotaRecord => handleQuotaRecord(record)
          case _ => // Only care about quota records
        }
      case _ => // Only care about metadata events
    }
  }

  private def handleQuotaRecord(quotaRecord: QuotaRecord): Unit = {
    val entityMap = mutable.Map[String, String]()
    quotaRecord.entity().forEach { entityData =>
      // A null entity name indicates the default entity (represented as <default>)
      val nameOrDefault = Option(entityData.entityName()).getOrElse(ConfigEntityName.Default)
      entityData.entityType() match {
        case ClientQuotaEntity.USER => entityMap.put(ClientQuotaEntity.USER, nameOrDefault)
        case ClientQuotaEntity.CLIENT_ID => entityMap.put(ClientQuotaEntity.CLIENT_ID, nameOrDefault)
        case ClientQuotaEntity.IP => entityMap.put(ClientQuotaEntity.IP, nameOrDefault)
      }
    }

    if (entityMap.contains(ClientQuotaEntity.IP)) {
      handleIpQuota(entityMap.get(ClientQuotaEntity.IP), quotaRecord)
    }

    if (entityMap.contains(ClientQuotaEntity.USER) || entityMap.contains(ClientQuotaEntity.CLIENT_ID)) {
      handleUserClientQuota(
        entityMap.get(ClientQuotaEntity.USER),
        entityMap.get(ClientQuotaEntity.CLIENT_ID),
        quotaRecord
      )
    }
  }

  private def handleIpQuota(ipName: Option[String], quotaRecord: QuotaRecord): Unit = {
    val inetAddress = try {
      ipName.map(s => InetAddress.getByName(s))
    } catch {
      case _: UnknownHostException => throw new IllegalArgumentException(s"Unable to resolve address $ipName")
    }

    val newValue = if (quotaRecord.remove()) {
      None
    } else {
      Some(quotaRecord.value).map(_.toInt)
    }

    connectionQuotas.updateIpConnectionRateQuota(inetAddress, newValue)
  }

  private def handleUserClientQuota(user: Option[String], clientId: Option[String], quotaRecord: QuotaRecord): Unit = {
    val managerOpt = quotaRecord.key() match {
      case QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG => Some(quotaManagers.fetch)
      case QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG => Some(quotaManagers.produce)
      case QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG => Some(quotaManagers.request)
      case QuotaConfigs.CONTROLLER_MUTATION_RATE_OVERRIDE_CONFIG => Some(quotaManagers.controllerMutation)
      case _ => warn(s"Unexpected quota key ${quotaRecord.key()}"); None
    }

    val quota = if (quotaRecord.remove()) {
      None
    } else {
      Some(new Quota(quotaRecord.value(), true))
    }

    managerOpt.foreach {
      // User and client id are not sanitized in the metadata record, so do that here
      manager => manager.updateQuota(
        sanitizedUser = user.map(Sanitizer.sanitize),
        clientId = clientId,
        sanitizedClientId = clientId.map(Sanitizer.sanitize),
        quota = quota)
    }
  }
}
