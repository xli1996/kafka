package kafka.server.metadata

import kafka.network.ConnectionQuotas
import kafka.server.{KafkaConfig, QuotaFactory}
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.common.config.internals.QuotaConfigs
import org.apache.kafka.common.metadata.QuotaRecord
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.quota.{ClientQuotaEntity, ClientQuotaFilter, ClientQuotaFilterComponent}
import org.junit.Test
import org.mockito.Mockito.mock

import java.util.Properties
import scala.jdk.CollectionConverters._


class QuotaMetadataProcessorTest {
  @Test
  def testProcessRecords(): Unit = {
    val configs = TestUtils.createBrokerConfigs(1, TestUtils.MockZkConnect)
      .map(KafkaConfig.fromProps(_, new Properties()))

    val time = new MockTime
    val metrics = new Metrics
    val quotaManagers = QuotaFactory.instantiate(configs.head, metrics, time, "quota-metadata-processor-test")
    val connectionQuotas = mock(classOf[ConnectionQuotas])
    val processor = new QuotaMetadataProcessor(quotaManagers, connectionQuotas)

    setupDescribeTest(processor)
    System.err.println(processor.quotaCache)
    System.err.println(processor.userQuotasIndex)
    System.err.println(processor.clientQuotasIndex)

    val filter = ClientQuotaFilter.containsOnly(
      List(
        ClientQuotaFilterComponent.ofEntity("client-id", "client-id-1"),
        ClientQuotaFilterComponent.ofDefaultEntity("user"),
      ).asJava)

    System.err.println(processor.describeClientQuotas(filter))
  }

  def setupDescribeTest(processor: QuotaMetadataProcessor): Unit = {
    addQuotaRecord(processor, userClientEntity("user-1", "client-id-1"),
      (QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 50.50))
    addQuotaRecord(processor, userClientEntity("user-2", "client-id-1"),
      (QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 51.51))
    addQuotaRecord(processor, userClientEntity("user-3", "client-id-2"),
      (QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 52.52))
    addQuotaRecord(processor, userClientEntity(null, "client-id-1"),
      (QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 53.53))
    addQuotaRecord(processor, userClientEntity("user-1", null),
      (QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 54.54))
  }

  def addQuotaRecord(processor: QuotaMetadataProcessor, entity: List[QuotaRecord.EntityData], quota: (String, Double)): Unit = {
    processor.handleQuotaRecord(new QuotaRecord()
      .setEntity(entity.asJava)
      .setKey(quota._1)
      .setValue(quota._2))
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
