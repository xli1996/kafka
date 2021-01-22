package kafka.testkit.junit;

import kafka.network.SocketServer;
import kafka.server.Kip500Broker;
import kafka.server.Kip500Controller;
import kafka.testkit.KafkaClusterTestKit;
import kafka.testkit.TestKitNodes;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.metadata.BrokerState;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class QuorumClusterInvocationContext implements TestTemplateInvocationContext {

    private final ClusterConfig clusterConfig;
    private final AtomicReference<KafkaClusterTestKit> clusterReference;

    public QuorumClusterInvocationContext(ClusterConfig clusterConfig) {
        this.clusterConfig = clusterConfig;
        this.clusterReference = new AtomicReference<>();
    }

    @Override
    public String getDisplayName(int invocationIndex) {
        String clusterDesc = clusterConfig.nameTags().entrySet().stream()
                .map(Object::toString)
                .collect(Collectors.joining(", "));
        return String.format("[Quorum %d] %s", invocationIndex, clusterDesc);
    }

    @Override
    public List<Extension> getAdditionalExtensions() {
        return Arrays.asList(
                (BeforeTestExecutionCallback) context -> {
                    KafkaClusterTestKit.Builder builder = new KafkaClusterTestKit.Builder(
                            new TestKitNodes.Builder().
                                    setNumKip500BrokerNodes(clusterConfig.brokers()).
                                    setNumControllerNodes(clusterConfig.controllers()).build());

                    // Copy properties into the TestKit builder
                    clusterConfig.serverProperties().forEach((key, value) -> builder.setConfigProp(key.toString(), value.toString()));

                    KafkaClusterTestKit cluster = builder.build();
                    clusterReference.set(cluster);
                    cluster.format();
                    cluster.startup();
                    kafka.utils.TestUtils.waitUntilTrue(
                            () -> cluster.kip500Brokers().get(0).currentState() == BrokerState.RUNNING,
                            () -> "Broker never made it to RUNNING state.",
                            org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS,
                            100L);
                },
                (AfterTestExecutionCallback) context -> {
                    clusterReference.get().close();
                },
                new ClusterInstanceParameterResolver(new QuorumClusterInstance(clusterReference, clusterConfig)),
                new ClusterConfigParameterResolver(clusterConfig),
                new IntegrationTestHelperParameterResolver()
        );
    }

    public static class QuorumClusterInstance implements ClusterInstance {

        private final AtomicReference<KafkaClusterTestKit> clusterReference;
        private final ClusterConfig clusterConfig;

        QuorumClusterInstance(AtomicReference<KafkaClusterTestKit> clusterReference, ClusterConfig clusterConfig) {
            this.clusterReference = clusterReference;
            this.clusterConfig = clusterConfig;
        }

        @Override
        public Collection<SocketServer> brokers() {
            return clusterReference.get().kip500Brokers().values().stream()
                    .map(Kip500Broker::socketServer)
                    .collect(Collectors.toList());
        }

        @Override
        public ListenerName listener() {
            return ListenerName.normalised("EXTERNAL");
        }

        @Override
        public Collection<SocketServer> controllers() {
            return clusterReference.get().controllers().values().stream()
                    .map(Kip500Controller::socketServer)
                    .collect(Collectors.toList());
        }

        @Override
        public ClusterType clusterType() {
            return ClusterType.Quorum;
        }

        @Override
        public ClusterConfig config() {
            return clusterConfig;
        }
    }
}
