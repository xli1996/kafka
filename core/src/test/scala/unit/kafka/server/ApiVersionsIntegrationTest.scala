package unit.kafka.server

import integration.kafka.server.IntegrationTestHelper
import kafka.testkit.ClusterHarness
import kafka.testkit.junit.{ClusterForEach, ClusterGenerator, ClusterConfig, ClusterTemplate}
import org.apache.kafka.common.message.ApiVersionsRequestData
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionsResponseKey
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests._
import org.junit.Assert.{assertEquals, assertFalse, assertNotNull}
import org.junit.jupiter.api.extension._

import scala.jdk.CollectionConverters._


object ApiVersionsIntegrationTest {
  def generateTwoIBPs(generator: ClusterGenerator): Unit = {
    generator.accept(ClusterConfig.newBuilder()
      .name("Test 1")
      .ibp("2.6")
      .build())

    generator.accept(ClusterConfig.newBuilder()
      .name("Test 2")
      .ibp("2.7-IV2")
      .build())
  }
}

@ExtendWith(value = Array(classOf[ClusterForEach]))
class ApiVersionsIntegrationTest(helper: IntegrationTestHelper,
                                 harness: ClusterHarness) {

  def addSpamAndEggs(harness: ClusterHarness): Unit = {
    harness.config().serverProperties().put("spam", "eggs")
  }

  @ClusterTemplate(generateClusters = "generateTwoIBPs", extendProperties = "addSpamAndEggs")
  def testApiVersionsRequest(): Unit = {
    System.err.println(harness.config().serverProperties())
    val request = new ApiVersionsRequest.Builder().build()
    val apiVersionsResponse = sendApiVersionsRequest(request)
    validateApiVersionsResponse(apiVersionsResponse)
  }

  @ClusterTemplate
  def testApiVersionsRequestWithUnsupportedVersion(): Unit = {
    val apiVersionsRequest = new ApiVersionsRequest.Builder().build()
    val apiVersionsResponse = sendUnsupportedApiVersionRequest(apiVersionsRequest)
    assertEquals(Errors.UNSUPPORTED_VERSION.code(), apiVersionsResponse.data.errorCode())
    assertFalse(apiVersionsResponse.data.apiKeys().isEmpty)
    val apiVersion = apiVersionsResponse.data.apiKeys().find(ApiKeys.API_VERSIONS.id)
    assertEquals(ApiKeys.API_VERSIONS.id, apiVersion.apiKey())
    assertEquals(ApiKeys.API_VERSIONS.oldestVersion(), apiVersion.minVersion())
    assertEquals(ApiKeys.API_VERSIONS.latestVersion(), apiVersion.maxVersion())
  }

  @ClusterTemplate
  def testApiVersionsRequestValidationV0(): Unit = {
    val apiVersionsRequest = new ApiVersionsRequest.Builder().build(0.asInstanceOf[Short])
    val apiVersionsResponse = sendApiVersionsRequest(apiVersionsRequest)
    validateApiVersionsResponse(apiVersionsResponse)
  }

  @ClusterTemplate
  def testApiVersionsRequestValidationV3(): Unit = {
    // Invalid request because Name and Version are empty by default
    val apiVersionsRequest = new ApiVersionsRequest(new ApiVersionsRequestData(), 3.asInstanceOf[Short])
    val apiVersionsResponse = sendApiVersionsRequest(apiVersionsRequest)
    assertEquals(Errors.INVALID_REQUEST.code(), apiVersionsResponse.data.errorCode())
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
