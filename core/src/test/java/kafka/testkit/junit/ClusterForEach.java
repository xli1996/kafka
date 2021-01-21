package kafka.testkit.junit;

import kafka.api.IntegrationTestHarness;
import kafka.network.SocketServer;
import kafka.server.Kip500Broker;
import kafka.server.Kip500Controller;
import kafka.server.LegacyBroker;
import kafka.testkit.ClusterHarness;
import kafka.testkit.KafkaClusterTestKit;
import kafka.testkit.TestKitNodes;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.metadata.BrokerState;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.junit.platform.commons.util.ReflectionUtils;
import scala.Option;
import scala.jdk.javaapi.CollectionConverters;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Template that creates two test invocations: one for legacy cluster and one for quorum clusters.
 *
 * In addition to providing the BeforeEach and AfterEach extensions for these generated invocations, this provider
 * also binds two parameters which can be injected into test methods or class constructors:
 *
 * <ul>
 *     <li>IntegrationTestHelper: Utility class of common network functions needed by integration tests</li>
 *     <li>ClusterHarness: Interface that exposes aspects of the underlying cluster</li>
 * </ul>
 */
public class ClusterForEach implements TestTemplateInvocationContextProvider {
    @Override
    public boolean supportsTestTemplate(ExtensionContext context) {
        return true;
    }

    void loadClusterProperties(ExtensionContext context, String propertySupplierMethod, Properties clusterProperties) {
        Object testInstance = context.getTestInstance().orElse(null);
        Method method = ReflectionUtils.getRequiredMethod(context.getRequiredTestClass(), propertySupplierMethod);
        final Object result;
        try {
            result = ReflectionUtils.makeAccessible(method).invoke(testInstance);
        } catch (Exception e) {
            throw new RuntimeException("Could not execute method " + propertySupplierMethod, e);
        }
        if (result instanceof Properties) {
            clusterProperties.putAll((Properties) result);
        } else {
            throw new IllegalArgumentException("Expected method " + propertySupplierMethod + " to return java.util.Properties");
        }
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context) {
        // Process the @ClusterTemplate annotation
        ClusterTemplate annot = context.getRequiredTestMethod().getDeclaredAnnotation(ClusterTemplate.class);
        if (annot == null) {
            throw new IllegalStateException("Please use @ClusterTemplate on test template methods when using the ClusterForEach provider");
        }
        SecurityProtocol securityProtocol = SecurityProtocol.forName(annot.securityProtocol());
        ListenerName listenerName;
        if (annot.listener().isEmpty()) {
            listenerName = ListenerName.forSecurityProtocol(securityProtocol);
        } else {
            listenerName = ListenerName.normalised(annot.listener());
        }

        // Return the two contexts
        return Stream.of(
            legacyContext(1, securityProtocol, listenerName, annot.serverProperties()),
            kip500Context(1, 1, securityProtocol, listenerName, annot.serverProperties())
        );
    }

    private TestTemplateInvocationContext legacyContext(
            int brokers,
            SecurityProtocol securityProtocol,
            ListenerName listenerName,
            String legacyPropertiesMethod) {

        final AtomicReference<IntegrationTestHarness> clusterReference = new AtomicReference<>();

        ClusterHarness harness = new ClusterHarness() {
            @Override
            public Collection<SocketServer> brokers() {
                return CollectionConverters.asJava(clusterReference.get().servers()).stream()
                    .map(LegacyBroker::socketServer)
                    .collect(Collectors.toList());
            }

            @Override
            public ListenerName listener() {
                return listenerName;
            }

            @Override
            public Collection<SocketServer> controllers() {
                return CollectionConverters.asJava(clusterReference.get().servers()).stream()
                    .filter(broker -> broker.legacyController().isDefined())
                    .map(LegacyBroker::socketServer)
                    .collect(Collectors.toList());
            }
        };

        return new TestTemplateInvocationContext() {
            @Override
            public String getDisplayName(int invocationIndex) {
                return "legacy";
            }

            @Override
            public List<Extension> getAdditionalExtensions() {
                return Arrays.asList(
                    (BeforeTestExecutionCallback) context -> {
                        Properties legacyProperties = new Properties();
                        if (legacyPropertiesMethod != null && !legacyPropertiesMethod.isEmpty()) {
                            loadClusterProperties(context, legacyPropertiesMethod, legacyProperties);
                        }

                        IntegrationTestHarness cluster = new IntegrationTestHarness() { // extends KafkaServerTestHarness
                            @Override
                            public SecurityProtocol securityProtocol() {
                                return securityProtocol;
                            }

                            @Override
                            public ListenerName listenerName() {
                                return listenerName;
                            }

                            @Override
                            public Option<Properties> serverSaslProperties() {
                                return Option.apply(legacyProperties);
                            }

                            @Override
                            public Option<Properties> clientSaslProperties() {
                                // TODO
                                return super.clientSaslProperties();
                            }

                            @Override
                            public int brokerCount() {
                                return brokers;
                            }
                        };
                        clusterReference.set(cluster);
                        cluster.setUp();
                        kafka.utils.TestUtils.waitUntilTrue(
                            () -> CollectionConverters.asJava(cluster.servers()).get(0).currentState() == BrokerState.RUNNING,
                            () -> "Broker never made it to RUNNING state.",
                            org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS,
                            100L);
                    },
                    (AfterTestExecutionCallback) context -> {
                        clusterReference.get().tearDown();
                    },
                    new ClusterHarnessParameterResolver(harness),
                    new IntegrationTestHelperParameterResolver()
                );
            }
        };
    }

    private TestTemplateInvocationContext kip500Context(
            int brokers, int controllers,
            SecurityProtocol securityProtocol,
            ListenerName listenerName,
            String quorumPropertiesMethod) {
        final AtomicReference<KafkaClusterTestKit> clusterReference = new AtomicReference<>();

        ClusterHarness harness = new ClusterHarness() {
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
        };

        return new TestTemplateInvocationContext() {
            @Override
            public String getDisplayName(int invocationIndex) {
                return "kip-500";
            }

            @Override
            public List<Extension> getAdditionalExtensions() {
                return Arrays.asList(
                    (BeforeTestExecutionCallback) context -> {
                        KafkaClusterTestKit.Builder builder = new KafkaClusterTestKit.Builder(
                            new TestKitNodes.Builder().
                                    setNumKip500BrokerNodes(brokers).
                                    setNumControllerNodes(controllers).build());

                        if (quorumPropertiesMethod != null && !quorumPropertiesMethod.isEmpty()) {
                            Properties quorumProperties = new Properties();
                            loadClusterProperties(context, quorumPropertiesMethod, quorumProperties);
                            quorumProperties.forEach((key, value) -> builder.setConfigProp(key.toString(), value.toString()));
                        }

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
                    new ClusterHarnessParameterResolver(harness),
                    new IntegrationTestHelperParameterResolver()
                );
            }
        };
    }
}
