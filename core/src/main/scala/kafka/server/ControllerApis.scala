package kafka.server

import kafka.network.RequestChannel
import kafka.server.QuotaFactory.QuotaManagers
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.acl.AclOperation.CLUSTER_ACTION
import org.apache.kafka.common.errors.ApiException
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.AlterIsrRequest
import org.apache.kafka.common.resource.Resource.CLUSTER_NAME
import org.apache.kafka.common.resource.ResourceType.CLUSTER
import org.apache.kafka.common.utils.Time
import org.apache.kafka.controller.{Controller, LeaderAndIsr}
import org.apache.kafka.server.authorizer.Authorizer

import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * Request handler for Controller APIs
 */
class ControllerApis(requestChannel: RequestChannel,
                     authorizer: Option[Authorizer],
                     quotas: QuotaManagers,
                     time: Time,
                     val controller: Controller)
  extends AbstractRequestHandler(
    requestChannel = requestChannel,
    authorizer = authorizer,
    quotas = quotas,
    time = time) with Logging {

  override def handle(request: RequestChannel.Request): Unit = {
    try {
      request.header.apiKey match {
        case ApiKeys.ALTER_ISR => handleAlterIsrRequest(request)
          // TODO other APIs
        case _ => throw new ApiException(s"Unsupported ApiKey ${request.context.header.apiKey()}")
      }
    } catch {
      case e: FatalExitError => throw e
      case e: Throwable => handleError(request, e)
    } finally {

    }
  }

  def handleAlterIsrRequest(request: RequestChannel.Request): Unit = {
    val alterIsrRequest = request.body[AlterIsrRequest]
    if (!authorize(request.context, CLUSTER_ACTION, CLUSTER, CLUSTER_NAME)) {
      val isrsToAlter = mutable.Map[TopicPartition, LeaderAndIsr]()
      alterIsrRequest.data.topics.forEach { topicReq =>
        topicReq.partitions.forEach { partitionReq =>
          val tp = new TopicPartition(topicReq.name, partitionReq.partitionIndex)
          val newIsr = partitionReq.newIsr()
          isrsToAlter.put(tp, new LeaderAndIsr(
            alterIsrRequest.data.brokerId,
            partitionReq.leaderEpoch,
            newIsr,
            partitionReq.currentIsrVersion))
        }
      }

      controller.alterIsr(
        alterIsrRequest.data().brokerId(),
        alterIsrRequest.data().brokerEpoch(),
        isrsToAlter.asJava)
    }
  }
}
