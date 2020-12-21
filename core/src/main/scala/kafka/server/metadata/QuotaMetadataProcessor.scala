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
   * TODO
   * @param quotaFilter
   * @return
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
        case ExactMatch(user) => userQuotasIndex(SpecificUser(user)).toSet
        case DefaultMatch => userQuotasIndex(DefaultUser).toSet
        case TypeMatch => userQuotasIndex.values.flatten.toSet
      }
    } else {
      Set[QuotaEntity]()
    }

    val clientMatches: Set[QuotaEntity] = if (clientMatch.isDefined) {
      clientMatch.get match {
        case ExactMatch(clientId) => clientQuotasIndex(SpecificClientId(clientId)).toSet
        case DefaultMatch => clientQuotasIndex(DefaultClientId).toSet
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
          case IpEntity(ip) => true
          case DefaultIpEntity => true
          case UserEntity(user) => userMatch.isDefined
          case DefaultUserEntity => userMatch.isDefined
          case ClientIdEntity(clientId) => clientMatch.isDefined
          case DefaultClientIdEntity => clientMatch.isDefined
          case UserClientIdEntity(user, clientId) => userMatch.isDefined && clientMatch.isDefined
          case DefaultUserClientIdEntity(clientId) => userMatch.isDefined && clientMatch.isDefined
          case UserDefaultClientIdEntity(user) => userMatch.isDefined && clientMatch.isDefined
          case DefaultUserDefaultClientIdEntity => userMatch.isDefined && clientMatch.isDefined
        }
      }
    } else {
      userClientMatches
    }

    System.err.println(userClientMatches)
    System.err.println(filteredMatches)

    val resultsMap: Map[QuotaEntity, Map[String, Double]] = filteredMatches.map {
      quotaEntity => quotaEntity -> quotaCache(quotaEntity).toMap
    }.toMap

    resultsMap
  }

  private[server] def handleQuotaRecord(quotaRecord: QuotaRecord): Unit = {
    // A mapping of entities in the record along with their names or <default> if the name was null
    val entityMap = mutable.Map[String, String]()
    quotaRecord.entity().forEach { entityData =>
      entityMap.put(entityData.entityType(), entityData.entityName())
      /*entityData.entityType() match {
        case ClientQuotaEntity.USER => entityMap.put(ClientQuotaEntity.USER, entityData.entityName())
        case ClientQuotaEntity.CLIENT_ID => entityMap.put(ClientQuotaEntity.CLIENT_ID, entityData.entityName())
        case ClientQuotaEntity.IP => entityMap.put(ClientQuotaEntity.IP, entityData.entityName())
      }*/
    }

    if (entityMap.contains(ClientQuotaEntity.IP)) {
      // IP quota manager has "default" convention of: None
      handleIpQuota(Option(entityMap(ClientQuotaEntity.IP)), quotaRecord)
    }

    if (entityMap.contains(ClientQuotaEntity.USER) || entityMap.contains(ClientQuotaEntity.CLIENT_ID)) {
      // User+Client quota managers has "default" convention of: Some("<default>")
      val userOpt = entityMap.get(ClientQuotaEntity.USER).map(s =>
        if (s == null) {
          ConfigEntityName.Default
        } else {
          s
        })

      val clientIdOpt = entityMap.get(ClientQuotaEntity.CLIENT_ID).map(s =>
        if (s == null) {
          ConfigEntityName.Default
        } else {
          s
        })

      handleUserClientQuota(
        userOpt,
        clientIdOpt,
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

  private def handleUserClientQuota(user: Option[String], clientId: Option[String], quotaRecord: QuotaRecord): Unit = {
    val quotaEntity = if (user.isDefined && clientId.isDefined) {
      if (user.get.equals(ConfigEntityName.Default) && clientId.get.equals(ConfigEntityName.Default)) {
        DefaultUserDefaultClientIdEntity
      } else if (user.get.equals(ConfigEntityName.Default)) {
        DefaultUserClientIdEntity(clientId.get)
      } else if (clientId.get.equals(ConfigEntityName.Default)) {
        UserDefaultClientIdEntity(user.get)
      } else {
        UserClientIdEntity(user.get, clientId.get)
      }
    } else if (user.isDefined) {
      if (user.get.equals(ConfigEntityName.Default)) {
        DefaultUserEntity
      } else {
        UserEntity(user.get)
      }
    } else {
      if (clientId.get.equals(ConfigEntityName.Default)) {
        DefaultClientIdEntity
      } else {
        ClientIdEntity(clientId.get)
      }
    }

    updateQuotaCache(quotaEntity, quotaRecord)

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
      case UserClientIdEntity(user, clientId) =>
        updateUserCache(SpecificUser(user), quotaEntity, quotaRecord.remove())
        updateClientIdCache(SpecificClientId(clientId), quotaEntity, quotaRecord.remove())
      case UserDefaultClientIdEntity(user) =>
        updateUserCache(SpecificUser(user), quotaEntity, quotaRecord.remove())
        updateClientIdCache(DefaultClientId, quotaEntity, quotaRecord.remove())
      case DefaultUserClientIdEntity(clientId) =>
        updateUserCache(DefaultUser, quotaEntity, quotaRecord.remove())
        updateClientIdCache(SpecificClientId(clientId), quotaEntity, quotaRecord.remove())
      case _ => // no indexing needed for other types
    }
  }
}
