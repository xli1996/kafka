package kafka.server.metadata

import kafka.network.ConnectionQuotas
import kafka.server.{ConfigEntityName, KafkaConfig, QuotaFactory}
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.common.config.internals.QuotaConfigs
import org.apache.kafka.common.metadata.QuotaRecord
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.quota.{ClientQuotaEntity, ClientQuotaFilter, ClientQuotaFilterComponent}
import org.junit.Assert.{assertEquals, assertFalse}
import org.junit.{Before, Test}
import org.mockito.Mockito.mock

import java.util.Properties
import scala.collection.mutable
import scala.jdk.CollectionConverters._


class QuotaMetadataProcessorTest {

  var processor: QuotaMetadataProcessor = _

  @Before
  def setup(): Unit = {
    val configs = TestUtils.createBrokerConfigs(1, TestUtils.MockZkConnect)
      .map(KafkaConfig.fromProps(_, new Properties()))

    val time = new MockTime
    val metrics = new Metrics
    val quotaManagers = QuotaFactory.instantiate(configs.head, metrics, time, "quota-metadata-processor-test")
    val connectionQuotas = mock(classOf[ConnectionQuotas])
    processor = new QuotaMetadataProcessor(quotaManagers, connectionQuotas)
  }

  @Test
  def testDescribeMatchExact(): Unit = {
    setupAndVerify(processor, { case (entity, _) =>
      val components = mutable.ListBuffer[ClientQuotaFilterComponent]()
      entityToFilter(entity, components.addOne)
      val filter = ClientQuotaFilter.containsOnly(components.toList.asJava)
      val results = processor.describeClientQuotas(filter)
      assertEquals(s"Should only match one quota for ${entity}", 1, results.size)
    })

    val nonMatching = List(
      userClientEntity("user-1", "client-id-2"),
      userClientEntity("user-3", "client-id-1"),
      userClientEntity("user-2", null),
      userEntity("user-4"),
      userClientEntity(null, "client-id-2"),
      clientEntity("client-id-1"),
      clientEntity("client-id-3")
    )

    nonMatching.foreach( entity => {
      val components = mutable.ListBuffer[ClientQuotaFilterComponent]()
      entityToFilter(entity, components.addOne)
      val filter = ClientQuotaFilter.containsOnly(components.toList.asJava)
      val results = processor.describeClientQuotas(filter)
      assertEquals(0, results.size)
    })
  }

  @Test
  def testDescribeMatchPartial(): Unit = {
    setupAndVerify(processor, { case (_, _) => })

    // Match open-ended existing user.
    val components = mutable.ListBuffer[ClientQuotaFilterComponent]()
    entityToFilter(userEntity("user-1"), components.addOne)
    var results = processor.describeClientQuotas(ClientQuotaFilter.contains(components.toList.asJava))
    assertEquals(3, results.size)
    assertEquals(3, results.keySet.count(quotaEntity => quotaEntity match {
      case UserEntity(user) => user.equals("user-1")
      case UserDefaultClientIdEntity(user) => user.equals("user-1")
      case UserClientIdEntity(user, _) => user.equals("user-1")
      case _ => false
    }))

    // Match open-ended non-existent user.
    components.clear()
    entityToFilter(userEntity("unknown"), components.addOne)
    results = processor.describeClientQuotas(ClientQuotaFilter.contains(components.toList.asJava))
    assertEquals(0, results.size)

    // Match open-ended existing client ID.
    components.clear()
    entityToFilter(clientEntity("client-id-2"), components.addOne)
    results = processor.describeClientQuotas(ClientQuotaFilter.contains(components.toList.asJava))
    assertEquals(2, results.size)
    assertEquals(2, results.keySet.count(quotaEntity => quotaEntity match {
      case ClientIdEntity(clientId) => clientId.equals("client-id-2")
      case DefaultUserClientIdEntity(clientId) => clientId.equals("client-id-2")
      case UserClientIdEntity(_, clientId) => clientId.equals("client-id-2")
      case _ => false
    }))
  }

  @Test
  def testEntityWithDefaultName(): Unit = {
    addQuotaRecord(processor, clientEntity(ConfigEntityName.Default), (QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 20000.0))
    addQuotaRecord(processor, clientEntity(null), (QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 30000.0))

    val components = mutable.ListBuffer[ClientQuotaFilterComponent]()
    entityToFilter(clientEntity(ConfigEntityName.Default), components.addOne)
    var results = processor.describeClientQuotas(ClientQuotaFilter.containsOnly(components.toList.asJava))
    assertEquals(1, results.size)

    components.clear()
    entityToFilter(clientEntity(null), components.addOne)
    results = processor.describeClientQuotas(ClientQuotaFilter.containsOnly(components.toList.asJava))
    assertEquals(1, results.size)
  }

  @Test
  def testQuotaRemoval(): Unit = {
    val entity = userClientEntity("user", "client-id")
    addQuotaRecord(processor, entity, (QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 10000.0))
    addQuotaRecord(processor, entity, (QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, 20000.0))
    var quotas = describeEntity(entity)
    assertEquals(2, quotas.size)
    assertEquals(10000.0, quotas(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG), 1e-6)

    addQuotaRecord(processor, entity, (QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, 10001.0))
    quotas = describeEntity(entity)
    assertEquals(2, quotas.size)
    assertEquals(10001.0, quotas(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG), 1e-6)

    addQuotaRemovalRecord(processor, entity, QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG)
    quotas = describeEntity(entity)
    assertEquals(1, quotas.size)
    assertFalse(quotas.contains(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG))

    addQuotaRemovalRecord(processor, entity, QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG)
    quotas = describeEntity(entity)
    assertEquals(0, quotas.size)
  }

  def describeEntity(entity: List[QuotaRecord.EntityData]): Map[String, Double] = {
    val components = mutable.ListBuffer[ClientQuotaFilterComponent]()
    entityToFilter(entity, components.addOne)
    val results = processor.describeClientQuotas(ClientQuotaFilter.containsOnly(components.toList.asJava))
    if (results.isEmpty) {
      Map()
    } else if (results.size == 1) {
      results.head._2
    } else {
      throw new AssertionError("Matched more than one entity with strict=true describe filter")
    }
  }

  def setupAndVerify(processor: QuotaMetadataProcessor,
                     verifier: (List[QuotaRecord.EntityData], (String, Double)) => Unit ): Unit = {
    val toVerify = List(
      (userClientEntity("user-1", "client-id-1"), 50.50),
      (userClientEntity("user-2", "client-id-1"), 51.51),
      (userClientEntity("user-3", "client-id-2"), 52.52),
      (userClientEntity(null, "client-id-1"), 53.53),
      (userClientEntity("user-1", null), 54.54),
      (userClientEntity("user-3", null), 55.55),
      (userEntity("user-1"), 56.56),
      (userEntity("user-2"), 57.57),
      (userEntity("user-3"), 58.58),
      (userEntity(null), 59.59),
      (clientEntity("client-id-2"), 60.60),
      (userClientEntity(null, null), 61.61)
    )

    toVerify.foreach {
      case (entity, value) => addQuotaRecord(processor, entity, (QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, value))
    }

    toVerify.foreach {
      case (entity, value) => verifier.apply(entity, (QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, value))
    }
  }

  def addQuotaRecord(processor: QuotaMetadataProcessor, entity: List[QuotaRecord.EntityData], quota: (String, Double)): Unit = {
    processor.handleQuotaRecord(new QuotaRecord()
      .setEntity(entity.asJava)
      .setKey(quota._1)
      .setValue(quota._2))
  }

  def addQuotaRemovalRecord(processor: QuotaMetadataProcessor, entity: List[QuotaRecord.EntityData], quota: String): Unit = {
    processor.handleQuotaRecord(new QuotaRecord()
      .setEntity(entity.asJava)
      .setKey(quota)
      .setRemove(true))
  }

  def entityToFilter(entity: List[QuotaRecord.EntityData], acceptor: ClientQuotaFilterComponent => Unit): Unit = {
    entity.foreach(entityData => {
      if (entityData.entityName() == null) {
        acceptor.apply(ClientQuotaFilterComponent.ofDefaultEntity(entityData.entityType()))
      } else {
        acceptor.apply(ClientQuotaFilterComponent.ofEntity(entityData.entityType(), entityData.entityName()))
      }
    })
  }

  def clientEntity(clientId: String): List[QuotaRecord.EntityData] = {
    List(new QuotaRecord.EntityData().setEntityType(ClientQuotaEntity.CLIENT_ID).setEntityName(clientId))
  }

  def userEntity(user: String): List[QuotaRecord.EntityData] = {
    List(new QuotaRecord.EntityData().setEntityType(ClientQuotaEntity.USER).setEntityName(user))
  }

  def userClientEntity(user: String, clientId: String): List[QuotaRecord.EntityData] = {
    List(
      new QuotaRecord.EntityData().setEntityType(ClientQuotaEntity.USER).setEntityName(user),
      new QuotaRecord.EntityData().setEntityType(ClientQuotaEntity.CLIENT_ID).setEntityName(clientId)
    )
  }

}
