package unit.kafka.server

import integration.kafka.server.IntegrationTestHelper
import kafka.api.{KafkaSasl, SaslSetup}
import kafka.testkit.junit.annotations.ClusterTest
import kafka.testkit.junit.{ClusterConfig, ClusterForEach, ClusterInstance}
import kafka.utils.JaasTestUtils
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionsResponseKey
import org.apache.kafka.common.message.SaslHandshakeRequestData
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{ApiVersionsRequest, ApiVersionsResponse, SaslHandshakeRequest, SaslHandshakeResponse}
import org.junit.Assert.{assertEquals, assertNotNull}
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.{AfterEach, BeforeEach}

import java.net.Socket
import java.util.Collections
import scala.jdk.CollectionConverters._

@ExtendWith(value = Array(classOf[ClusterForEach]))
class SaslApiVersionsIntegrationTest(helper: IntegrationTestHelper,
                                     harness: ClusterInstance) {

  //protected override val clientSaslProperties = Some(kafkaClientSaslProperties(kafkaClientSaslMechanism))

  val kafkaClientSaslMechanism = "PLAIN"
  val kafkaServerSaslMechanisms = List("PLAIN")

  private var sasl: SaslSetup = _

  @BeforeEach
  def setupSasl(config: ClusterConfig): Unit = {
    System.err.println("BeforeEach SASL setup")
    sasl = new SaslSetup() {}
    sasl.startSasl(sasl.jaasSections(kafkaServerSaslMechanisms, Some(kafkaClientSaslMechanism), KafkaSasl, JaasTestUtils.KafkaServerContextName))
    config.serverProperties().putAll(sasl.kafkaServerSaslProperties(kafkaServerSaslMechanisms, kafkaClientSaslMechanism))
  }

  @ClusterTest
  def testApiVersionsRequestBeforeSaslHandshakeRequest(): Unit = {
    val socket = helper.connect(harness.brokers().asScala.head, harness.listener())
    try {
      val apiVersionsResponse = helper.sendAndReceive[ApiVersionsResponse](
        new ApiVersionsRequest.Builder().build(0), socket)
      validateApiVersionsResponse(apiVersionsResponse)
      sendSaslHandshakeRequestValidateResponse(socket)
    } finally {
      socket.close()
    }
  }

  @AfterEach
  def closeSasl(): Unit = {
    System.err.println("AfterEach SASL setup")
    sasl.closeSasl()
  }

  private def sendSaslHandshakeRequestValidateResponse(socket: Socket): Unit = {
    val request = new SaslHandshakeRequest(new SaslHandshakeRequestData().setMechanism("PLAIN"),
      ApiKeys.SASL_HANDSHAKE.latestVersion)
    val response = helper.sendAndReceive[SaslHandshakeResponse](request, socket)
    assertEquals(Errors.NONE, response.error)
    assertEquals(Collections.singletonList("PLAIN"), response.enabledMechanisms)
  }

  def sendUnsupportedApiVersionRequest(request: ApiVersionsRequest): ApiVersionsResponse = {
    val overrideHeader = helper.nextRequestHeader(ApiKeys.API_VERSIONS, Short.MaxValue)
    val socket = helper.connect(harness.brokers().asScala.head, harness.listener())
    try {
      helper.sendWithHeader(request, overrideHeader, socket)
      helper.receive[ApiVersionsResponse](socket, ApiKeys.API_VERSIONS, 0.toShort)
    } finally socket.close()
  }

  def validateApiVersionsResponse(apiVersionsResponse: ApiVersionsResponse): Unit = {
    val enabledPublicApis = ApiKeys.enabledApis()
    assertEquals("API keys in ApiVersionsResponse must match API keys supported by broker.",
      enabledPublicApis.size(), apiVersionsResponse.data.apiKeys().size())
    for (expectedApiVersion: ApiVersionsResponseKey <- ApiVersionsResponse.DEFAULT_API_VERSIONS_RESPONSE.data.apiKeys().asScala) {
      val actualApiVersion = apiVersionsResponse.apiVersion(expectedApiVersion.apiKey)
      assertNotNull(s"API key ${actualApiVersion.apiKey} is supported by broker, but not received in ApiVersionsResponse.", actualApiVersion)
      assertEquals("API key must be supported by the broker.", expectedApiVersion.apiKey, actualApiVersion.apiKey)
      assertEquals(s"Received unexpected min version for API key ${actualApiVersion.apiKey}.", expectedApiVersion.minVersion, actualApiVersion.minVersion)
      assertEquals(s"Received unexpected max version for API key ${actualApiVersion.apiKey}.", expectedApiVersion.maxVersion, actualApiVersion.maxVersion)
    }
  }

  def sendApiVersionsRequest(request: ApiVersionsRequest): ApiVersionsResponse = {
    helper.connectAndReceive[ApiVersionsResponse](request, harness.brokers().asScala.head, harness.listener())
  }
}
