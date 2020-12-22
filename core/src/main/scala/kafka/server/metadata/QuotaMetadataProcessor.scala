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
import org.apache.kafka.common.errors.{InvalidRequestException, UnsupportedVersionException}
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

// A type for the cache index keys
sealed trait CacheIndexKey
case object DefaultUser extends CacheIndexKey
case class SpecificUser(user: String) extends CacheIndexKey
case object DefaultClientId extends CacheIndexKey
case class SpecificClientId(clientId: String) extends CacheIndexKey
case object DefaultIp extends CacheIndexKey
case class SpecificIp(ip: String) extends CacheIndexKey

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

  type QuotaCacheIndex = mutable.HashMap[CacheIndexKey, mutable.HashSet[QuotaEntity]]

  // A cache of the quota entities and their current quota values
  val quotaCache = new mutable.HashMap[QuotaEntity, mutable.Map[String, Double]]

  // An index of user or client to a set of corresponding cache entities. This is used for flexible lookups
  val userEntityIndex = new QuotaCacheIndex
  val clientIdEntityIndex = new QuotaCacheIndex
  val ipEntityIndex = new QuotaCacheIndex

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

    // Do some preliminary validation of the filter types and convert them to correct QuotaMatch type
    val entityFilters: mutable.Map[String, QuotaMatch] = mutable.HashMap.empty
    quotaFilter.components().forEach(component => {
      val entityType = component.entityType()
      if (entityFilters.contains(entityType)) {
        throw new InvalidRequestException(s"Duplicate ${entityType} filter component entity type")
      } else if (entityType.isEmpty) {
        throw new InvalidRequestException("Unexpected empty filter component entity type")
      } else if (!ClientQuotaEntity.isValidEntityType(entityType)) {
        throw new UnsupportedVersionException(s"Custom entity type ${entityType} not supported")
      }

      // A present "match()" is an exact match on name, an absent "match()" is a match on the default entity,
      // and a null "match()" is a match on the entity type
      val entityMatch = if (component.`match`() != null && component.`match`().isPresent) {
        ExactMatch(component.`match`().get())
      } else if (component.`match`() != null) {
        DefaultMatch
      } else {
        TypeMatch
      }
      entityFilters.put(entityType, entityMatch)
    })

    if (entityFilters.isEmpty) {
      // TODO return empty or throw exception here?
      return Map.empty
    }

    // We do not allow IP filters to be combined with user or client filters
    if (entityFilters.contains(ClientQuotaEntity.IP) && entityFilters.size > 1) {
      throw new InvalidRequestException("Invalid entity filter component combination, IP filter component should " +
        "not be used with user or clientId filter component.")
    }

    val ipMatch = entityFilters.get(ClientQuotaEntity.IP)
    val ipIndexMatches: Set[QuotaEntity] = if (ipMatch.isDefined) {
      ipMatch.get match {
        case ExactMatch(ip) => ipEntityIndex.getOrElse(SpecificIp(ip), Set()).toSet
        case DefaultMatch => ipEntityIndex.getOrElse(DefaultIp, Set()).toSet
        case TypeMatch => ipEntityIndex.values.flatten.toSet
      }
    } else {
      Set()
    }

    val userMatch = entityFilters.get(ClientQuotaEntity.USER)
    val userIndexMatches: Set[QuotaEntity] = if (userMatch.isDefined) {
      userMatch.get match {
        case ExactMatch(user) => userEntityIndex.getOrElse(SpecificUser(user), Set()).toSet
        case DefaultMatch => userEntityIndex.getOrElse(DefaultUser, Set()).toSet
        case TypeMatch => userEntityIndex.values.flatten.toSet
      }
    } else {
      Set()
    }

    val clientMatch = entityFilters.get(ClientQuotaEntity.CLIENT_ID)
    val clientIndexMatches: Set[QuotaEntity] = if (clientMatch.isDefined) {
      clientMatch.get match {
        case ExactMatch(clientId) => clientIdEntityIndex.getOrElse(SpecificClientId(clientId), Set()).toSet
        case DefaultMatch => clientIdEntityIndex.getOrElse(DefaultClientId, Set()).toSet
        case TypeMatch => clientIdEntityIndex.values.flatten.toSet
      }
    } else {
      Set()
    }

    val candidateMatches: Set[QuotaEntity] = if (userMatch.isDefined && clientMatch.isDefined) {
      userIndexMatches.intersect(clientIndexMatches)
    } else if (userMatch.isDefined) {
      userIndexMatches
    } else if (clientMatch.isDefined) {
      clientIndexMatches
    } else if (ipMatch.isDefined) {
      ipIndexMatches
    } else {
      // TODO Should not get here, maybe throw?
      Set()
    }

    val filteredMatches: Set[QuotaEntity] = if (quotaFilter.strict()) {
      // If in strict mode, need to remove any matches with extra entity types. This only applies to results with
      // both user and clientId parts
      candidateMatches.filter { quotaEntity =>
        quotaEntity match {
          case UserClientIdEntity(_, _) => userMatch.isDefined && clientMatch.isDefined
          case DefaultUserClientIdEntity(_) => userMatch.isDefined && clientMatch.isDefined
          case UserDefaultClientIdEntity(_) => userMatch.isDefined && clientMatch.isDefined
          case DefaultUserDefaultClientIdEntity => userMatch.isDefined && clientMatch.isDefined
          case _ => true
        }
      }
    } else {
      candidateMatches
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
      ipName.map(InetAddress.getByName)
    } catch {
      case _: UnknownHostException => throw new IllegalArgumentException(s"Unable to resolve address $ipName")
    }

    // Map to an appropriate entity
    val quotaEntity = if (ipName.isDefined) {
      IpEntity(ipName.get)
    } else {
      DefaultIpEntity
    }

    // The connection quota only understands the connection rate limit
    if (quotaRecord.key() != QuotaConfigs.IP_CONNECTION_RATE_OVERRIDE_CONFIG) {
      warn(s"Ignoring unexpected quota key ${quotaRecord.key()} for entity $quotaEntity")
      return
    }

    // Update the cache
    updateQuotaCache(quotaEntity, quotaRecord)

    // Convert the value to an appropriate Option for the quota manager
    val newValue = if (quotaRecord.remove()) {
      None
    } else {
      Some(quotaRecord.value).map(_.toInt)
    }
    connectionQuotas.updateIpConnectionRateQuota(inetAddress, newValue)
  }

  private def handleUserClientQuota(quotaEntity: QuotaEntity, quotaRecord: QuotaRecord): Unit = {
    val managerOpt = quotaRecord.key() match {
      case QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG => Some(quotaManagers.fetch)
      case QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG => Some(quotaManagers.produce)
      case QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG => Some(quotaManagers.request)
      case QuotaConfigs.CONTROLLER_MUTATION_RATE_OVERRIDE_CONFIG => Some(quotaManagers.controllerMutation)
      case _ => warn(s"Ignoring unexpected quota key ${quotaRecord.key()} for entity $quotaEntity"); None
    }

    if (managerOpt.isEmpty) {
      return
    }

    updateQuotaCache(quotaEntity, quotaRecord)

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
      case IpEntity(_) | DefaultIpEntity => (None, None) // TODO should not get here, maybe throw?
    }

    val quotaValue = if (quotaRecord.remove()) {
      None
    } else {
      Some(new Quota(quotaRecord.value(), true))
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
    val removeCache = if (quotaRecord.remove()) {
      quotaValues.remove(quotaRecord.key())
      if (quotaValues.isEmpty) {
        quotaCache.remove(quotaEntity)
        true
      } else {
        false
      }
    } else {
      quotaValues.put(quotaRecord.key(), quotaRecord.value())
      false
    }

    // Update the cache indexes for user/client quotas
    def updateCacheIndex(quotaEntity: QuotaEntity,
                         remove: Boolean)
                        (quotaCacheIndex: QuotaCacheIndex,
                         key: CacheIndexKey): Unit = {
      if (remove) {
        val cleanup = quotaCacheIndex.get(key) match {
          case Some(quotaEntitySet) => quotaEntitySet.remove(quotaEntity); quotaEntitySet.isEmpty
          case None => false
        }
        if (cleanup) {
          quotaCacheIndex.remove(key)
        }
      } else {
        quotaCacheIndex.getOrElseUpdate(key, mutable.HashSet.empty).add(quotaEntity)
      }
    }

    // Update the appropriate indexes with the entity
    val updateCacheIndexPartial: (QuotaCacheIndex, CacheIndexKey) => Unit = updateCacheIndex(quotaEntity, removeCache)
    quotaEntity match {
      case UserEntity(user) =>
        updateCacheIndexPartial(userEntityIndex, SpecificUser(user))
      case DefaultUserEntity =>
        updateCacheIndexPartial(userEntityIndex, DefaultUser)

      case ClientIdEntity(clientId) =>
        updateCacheIndexPartial(clientIdEntityIndex, SpecificClientId(clientId))
      case DefaultClientIdEntity =>
        updateCacheIndexPartial(clientIdEntityIndex, DefaultClientId)

      case UserClientIdEntity(user, clientId) =>
        updateCacheIndexPartial(userEntityIndex, SpecificUser(user))
        updateCacheIndexPartial(clientIdEntityIndex, SpecificClientId(clientId))

      case UserDefaultClientIdEntity(user) =>
        updateCacheIndexPartial(userEntityIndex, SpecificUser(user))
        updateCacheIndexPartial(clientIdEntityIndex, DefaultClientId)

      case DefaultUserClientIdEntity(clientId) =>
        updateCacheIndexPartial(userEntityIndex, DefaultUser)
        updateCacheIndexPartial(clientIdEntityIndex, SpecificClientId(clientId))

      case DefaultUserDefaultClientIdEntity =>
        updateCacheIndexPartial(userEntityIndex, DefaultUser)
        updateCacheIndexPartial(clientIdEntityIndex, DefaultClientId)

      case IpEntity(ip) =>
        updateCacheIndexPartial(ipEntityIndex, SpecificIp(ip))
      case DefaultIpEntity =>
        updateCacheIndexPartial(ipEntityIndex, DefaultIp)
    }
  }
}
