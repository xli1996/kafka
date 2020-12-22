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
import kafka.server.ConfigEntityName
import kafka.server.QuotaFactory.QuotaManagers
import kafka.utils.Logging
import org.apache.kafka.common.config.internals.QuotaConfigs
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.metadata.QuotaRecord
import org.apache.kafka.common.metrics.Quota
import org.apache.kafka.common.quota.{ClientQuotaEntity, ClientQuotaFilter}
import org.apache.kafka.common.utils.Sanitizer

import java.net.{InetAddress, UnknownHostException}
import scala.collection.mutable

// Types for primary quota cache
sealed trait QuotaEntity
case class IpEntity(ip: String) extends QuotaEntity
case object DefaultIpEntity extends QuotaEntity

case class UserEntity(user: String) extends QuotaEntity
case object DefaultUserEntity extends QuotaEntity

case class ClientIdEntity(clientId: String) extends QuotaEntity
case object DefaultClientIdEntity extends QuotaEntity

case class UserClientIdEntity(user: String, clientId: String) extends QuotaEntity
case class UserDefaultClientIdEntity(user: String) extends QuotaEntity
case class DefaultUserClientIdEntity(clientId: String) extends QuotaEntity
case object DefaultUserDefaultClientIdEntity extends QuotaEntity

// User cache keys
sealed trait User
case object DefaultUser extends User
case class SpecificUser(user: String) extends User

// ClientId cache keys
sealed trait ClientId
case object DefaultClientId extends ClientId
case class SpecificClientId(clientId: String) extends ClientId

// Different types of matching constraints
sealed trait QuotaMatch
case class ExactMatch(entityName: String) extends QuotaMatch
case object DefaultMatch extends QuotaMatch
case object TypeMatch extends QuotaMatch

/**
 * Watch for changes to quotas in the metadata log and update quota managers as necessary
 *
 * @param quotaManagers
 * @param connectionQuotas
 */
class QuotaMetadataProcessor(val quotaManagers: QuotaManagers,
                             val connectionQuotas: ConnectionQuotas) extends BrokerMetadataProcessor with Logging {

  val userQuotasIndex = new mutable.HashMap[User, mutable.HashSet[QuotaEntity]]
  val clientQuotasIndex = new mutable.HashMap[ClientId, mutable.HashSet[QuotaEntity]]
  val quotaCache = new mutable.HashMap[QuotaEntity, mutable.Map[String, Double]]

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


  /**
   * Return quota entries for a given filter. These entries are returned from an in-memory cache and may not reflect
   * the latest state of the quotas according to the controller.
   *
   * @param quotaFilter       A quota entity filter
   * @return                  A mapping of quota entities along with their quota values
   */
  def describeClientQuotas(quotaFilter: ClientQuotaFilter): Map[QuotaEntity, Map[String, Double]] = {

    val entityFilters: mutable.Map[String, QuotaMatch] = mutable.HashMap.empty

     quotaFilter.components().forEach(component => {
       val entityType = component.entityType()
       if (entityFilters.contains(entityType)) {
         throw new InvalidRequestException(s"Duplicate ${entityType} filter component entity type")
       } else if (entityType.isEmpty) {
         throw new InvalidRequestException("Unexpected empty filter component entity type")
       } else if (!ClientQuotaEntity.isValidEntityType(entityType)) {
         throw new InvalidRequestException(s"Custom entity type ${entityType} not supported")
       }

       val entityMatch = if (component.`match`() != null && component.`match`().isPresent) {
         ExactMatch(component.`match`().get())
       } else if (component.`match`() != null) {
         DefaultMatch
       } else {
         TypeMatch
       }
       entityFilters.put(entityType, entityMatch)
     })

    if (entityFilters.contains(ClientQuotaEntity.IP) && entityFilters.size > 1) {
      new InvalidRequestException("Invalid entity filter component combination, IP filter component should not be " +
        "used with user or clientId filter component.")
    }

    // user + client

    val userMatch = entityFilters.get(ClientQuotaEntity.USER)
    val clientMatch = entityFilters.get(ClientQuotaEntity.CLIENT_ID)

    val userMatches: Set[QuotaEntity] = if (userMatch.isDefined) {
      userMatch.get match {
        case ExactMatch(user) => userQuotasIndex.getOrElse(SpecificUser(user), Set()).toSet
        case DefaultMatch => userQuotasIndex.getOrElse(DefaultUser, Set()).toSet
        case TypeMatch => userQuotasIndex.values.flatten.toSet
      }
    } else {
      Set[QuotaEntity]()
    }

    val clientMatches: Set[QuotaEntity] = if (clientMatch.isDefined) {
      clientMatch.get match {
        case ExactMatch(clientId) => clientQuotasIndex.getOrElse(SpecificClientId(clientId), Set()).toSet
        case DefaultMatch => clientQuotasIndex.getOrElse(DefaultClientId, Set()).toSet
        case TypeMatch => clientQuotasIndex.values.flatten.toSet
      }
    } else {
      Set[QuotaEntity]()
    }

    // TODO ip match

    val userClientMatches = if (userMatch.isDefined && clientMatch.isDefined) {
      userMatches.intersect(clientMatches)
    } else if (userMatch.isDefined) {
      userMatches
    } else if (clientMatch.isDefined) {
      clientMatches
    } else {
      // shouldn't get here
      Set[QuotaEntity]()
    }



    val filteredMatches: Set[QuotaEntity] = if (quotaFilter.strict()) {
      // Remove any matches with extra entity types
      userClientMatches.filter { quotaEntity =>
        quotaEntity match {
          case IpEntity(_) => true
          case DefaultIpEntity => true
          case UserEntity(_) => userMatch.isDefined
          case DefaultUserEntity => userMatch.isDefined
          case ClientIdEntity(_) => clientMatch.isDefined
          case DefaultClientIdEntity => clientMatch.isDefined
          case UserClientIdEntity(_, _) => userMatch.isDefined && clientMatch.isDefined
          case DefaultUserClientIdEntity(_) => userMatch.isDefined && clientMatch.isDefined
          case UserDefaultClientIdEntity(_) => userMatch.isDefined && clientMatch.isDefined
          case DefaultUserDefaultClientIdEntity => userMatch.isDefined && clientMatch.isDefined
        }
      }
    } else {
      userClientMatches
    }

    val resultsMap: Map[QuotaEntity, Map[String, Double]] = filteredMatches.map {
      quotaEntity => quotaEntity -> quotaCache(quotaEntity).toMap
    }.toMap

    resultsMap
  }

  private[server] def handleQuotaRecord(quotaRecord: QuotaRecord): Unit = {
    val entityMap = mutable.Map[String, String]()
    quotaRecord.entity().forEach { entityData =>
      entityMap.put(entityData.entityType(), entityData.entityName())
    }

    if (entityMap.contains(ClientQuotaEntity.IP)) {
      // In the IP quota manager, None is used for default entity
      handleIpQuota(Option(entityMap(ClientQuotaEntity.IP)), quotaRecord)
      return
    }

    if (entityMap.contains(ClientQuotaEntity.USER) || entityMap.contains(ClientQuotaEntity.CLIENT_ID)) {
      // Need to handle null values for default entity name, so use "getOrElse" combined with "contains" checks
      val userVal = entityMap.getOrElse(ClientQuotaEntity.USER, null)
      val clientIdVal = entityMap.getOrElse(ClientQuotaEntity.CLIENT_ID, null)

      // In User+Client quota managers, "<default>" is used for default entity, so we need to represent all possible
      // combinations of values, defaults, and absent entities
      val userClientEntity = if (entityMap.contains(ClientQuotaEntity.USER) && entityMap.contains(ClientQuotaEntity.CLIENT_ID)) {
        if (userVal == null && clientIdVal == null) {
          DefaultUserDefaultClientIdEntity
        } else if (userVal == null) {
          DefaultUserClientIdEntity(clientIdVal)
        } else if (clientIdVal == null) {
          UserDefaultClientIdEntity(userVal)
        } else {
          UserClientIdEntity(userVal, clientIdVal)
        }
      } else if (entityMap.contains(ClientQuotaEntity.USER)) {
        if (userVal == null) {
          DefaultUserEntity
        } else {
          UserEntity(userVal)
        }
      } else {
        if (clientIdVal == null) {
          DefaultClientIdEntity
        } else {
          ClientIdEntity(clientIdVal)
        }
      }
      handleUserClientQuota(
        userClientEntity,
        quotaRecord
      )
      return
    }
  }

  private def handleIpQuota(ipName: Option[String], quotaRecord: QuotaRecord): Unit = {
    val inetAddress = try {
      ipName.map(s => InetAddress.getByName(s))
    } catch {
      case _: UnknownHostException => throw new IllegalArgumentException(s"Unable to resolve address $ipName")
    }

    val quotaEntity = if (inetAddress.isDefined) {
      IpEntity(inetAddress.get.toString)
    } else {
      DefaultIpEntity
    }

    updateQuotaCache(quotaEntity, quotaRecord)

    val newValue = if (quotaRecord.remove()) {
      None
    } else {
      Some(quotaRecord.value).map(_.toInt)
    }

    connectionQuotas.updateIpConnectionRateQuota(inetAddress, newValue)
  }

  private def handleUserClientQuota(quotaEntity: QuotaEntity, quotaRecord: QuotaRecord): Unit = {
    updateQuotaCache(quotaEntity, quotaRecord)

    val managerOpt = quotaRecord.key() match {
      case QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG => Some(quotaManagers.fetch)
      case QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG => Some(quotaManagers.produce)
      case QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG => Some(quotaManagers.request)
      case QuotaConfigs.CONTROLLER_MUTATION_RATE_OVERRIDE_CONFIG => Some(quotaManagers.controllerMutation)
      case _ => warn(s"Unexpected quota key ${quotaRecord.key()}"); None
    }

    val quotaValue = if (quotaRecord.remove()) {
      None
    } else {
      Some(new Quota(quotaRecord.value(), true))
    }

    // Convert entity into Options with sanitized values for QuotaManagers
    val (sanitizedUser, sanitizedClientId) = quotaEntity match {
      case UserEntity(user) => (Some(Sanitizer.sanitize(user)), None)
      case DefaultUserEntity => (Some(ConfigEntityName.Default), None)
      case ClientIdEntity(clientId) => (None, Some(Sanitizer.sanitize(clientId)))
      case DefaultClientIdEntity => (None, Some(ConfigEntityName.Default))
      case UserClientIdEntity(user, clientId) => (Some(Sanitizer.sanitize(user)), Some(Sanitizer.sanitize(clientId)))
      case UserDefaultClientIdEntity(user) => (Some(Sanitizer.sanitize(user)), Some(ConfigEntityName.Default))
      case DefaultUserClientIdEntity(clientId) => (Some(ConfigEntityName.Default), Some(Sanitizer.sanitize(clientId)))
      case DefaultUserDefaultClientIdEntity => (Some(ConfigEntityName.Default), Some(ConfigEntityName.Default))
      case IpEntity(_) | DefaultIpEntity => (None, None)
    }

    managerOpt.foreach {
      manager => manager.updateQuota(
        sanitizedUser = sanitizedUser,
        clientId = sanitizedClientId.map(Sanitizer.desanitize),
        sanitizedClientId = sanitizedClientId,
        quota = quotaValue)
    }
  }

  private def updateQuotaCache(quotaEntity: QuotaEntity, quotaRecord: QuotaRecord): Unit = {
    // Update the quota entity map
    val quotaValues = quotaCache.getOrElseUpdate(quotaEntity, mutable.HashMap.empty)
    if (quotaRecord.remove()) {
      quotaValues.remove(quotaRecord.key())
      if (quotaValues.isEmpty) {
        quotaCache.remove(quotaEntity)
      }
    } else {
      quotaValues.put(quotaRecord.key(), quotaRecord.value())
    }

    // Update the cache indexes for user/client quotas
    def updateUserCache(user: User,
                        quotaEntity: QuotaEntity,
                        remove: Boolean): Unit = {
      if (remove) {
        userQuotasIndex.get(user).foreach(_.remove(quotaEntity))
      } else {
        userQuotasIndex.getOrElseUpdate(user, mutable.HashSet.empty).add(quotaEntity)
      }
    }

    def updateClientIdCache(clientId: ClientId,
                            quotaEntity: QuotaEntity,
                            remove: Boolean): Unit = {
      if (remove) {
        clientQuotasIndex.get(clientId).foreach(_.remove(quotaEntity))
      } else {
        clientQuotasIndex.getOrElseUpdate(clientId, mutable.HashSet.empty).add(quotaEntity)
      }
    }

    quotaEntity match {
      case UserEntity(user) =>
        updateUserCache(SpecificUser(user), quotaEntity, quotaRecord.remove())
      case DefaultUserEntity =>
        updateUserCache(DefaultUser, quotaEntity, quotaRecord.remove())

      case ClientIdEntity(clientId) =>
        updateClientIdCache(SpecificClientId(clientId), quotaEntity, quotaRecord.remove())
      case DefaultClientIdEntity =>
        updateClientIdCache(DefaultClientId, quotaEntity, quotaRecord.remove())

      case UserClientIdEntity(user, clientId) =>
        updateUserCache(SpecificUser(user), quotaEntity, quotaRecord.remove())
        updateClientIdCache(SpecificClientId(clientId), quotaEntity, quotaRecord.remove())

      case UserDefaultClientIdEntity(user) =>
        updateUserCache(SpecificUser(user), quotaEntity, quotaRecord.remove())
        updateClientIdCache(DefaultClientId, quotaEntity, quotaRecord.remove())

      case DefaultUserClientIdEntity(clientId) =>
        updateUserCache(DefaultUser, quotaEntity, quotaRecord.remove())
        updateClientIdCache(SpecificClientId(clientId), quotaEntity, quotaRecord.remove())

      case DefaultUserDefaultClientIdEntity =>
        updateUserCache(DefaultUser, quotaEntity, quotaRecord.remove())
        updateClientIdCache(DefaultClientId, quotaEntity, quotaRecord.remove())

      case IpEntity(_) | DefaultIpEntity => // Don't index these
    }
  }
}
