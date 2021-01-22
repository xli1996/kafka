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
import org.junit.platform.commons.util.ExceptionUtils;
import org.junit.platform.commons.util.ReflectionUtils;
import scala.Option;
import scala.jdk.javaapi.CollectionConverters;

import java.lang.reflect.Method;
import java.util.ArrayList;
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

    void extendPerTestProperties(ExtensionContext context,
                                 String propertySupplierMethod,
                                 ClusterHarness harness) {
        Object testInstance = context.getTestInstance().orElse(null);
        Method method = ReflectionUtils.getRequiredMethod(context.getRequiredTestClass(), propertySupplierMethod, ClusterHarness.class);
        try {
            ReflectionUtils.makeAccessible(method).invoke(testInstance, harness);
        } catch (Throwable t) {
            throw ExceptionUtils.throwAsUncheckedException(t);
        }
    }

    private void generateClusterConfigurations(ExtensionContext context, String generateClustersMethods, ClusterGenerator generator) {
        Object testInstance = context.getTestInstance().orElse(null);
        Method method = ReflectionUtils.getRequiredMethod(context.getRequiredTestClass(), generateClustersMethods, ClusterGenerator.class);
        ReflectionUtils.invokeMethod(method, testInstance, generator);
    }

    private ClusterConfig defaultCluster() {
        return ClusterConfig.newBuilder().name("default").build();
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

        List<ClusterConfig> generatedClusterConfigs = new ArrayList<>();
        if (!annot.generateClusters().isEmpty()) {
            generateClusterConfigurations(context, annot.generateClusters(), generatedClusterConfigs::add);
        } else {
            // Ensure we have at least one
            generatedClusterConfigs.add(defaultCluster());
        }

        return generatedClusterConfigs.stream().flatMap(clusterConfig -> Stream.of(
                legacyContext(1, securityProtocol, listenerName, clusterConfig.copyOf(), annot.extendProperties()),
                kip500Context(1, 1, securityProtocol, listenerName, clusterConfig.copyOf(), annot.extendProperties())
        ));
    }

    private TestTemplateInvocationContext legacyContext(
            int brokers,
            SecurityProtocol securityProtocol,
            ListenerName listenerName,
            ClusterConfig clusterConfig,
            String extendPropertiesMethod) {

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

            @Override
            public ClusterType type() {
                return ClusterType.Legacy;
            }

            @Override
            public ClusterConfig config() {
                return clusterConfig;
            }
        };

        return new TestTemplateInvocationContext() {
            @Override
            public String getDisplayName(int invocationIndex) {
                return "Legacy, " + clusterConfig.ibp().map(ibp -> String.format("IBP=%s, ", ibp)).orElse("") + "Name=" + clusterConfig.name();
            }

            @Override
            public List<Extension> getAdditionalExtensions() {
                return Arrays.asList(
                    (BeforeTestExecutionCallback) context -> {
                        if (extendPropertiesMethod != null && !extendPropertiesMethod.isEmpty()) {
                            extendPerTestProperties(context, extendPropertiesMethod, harness);
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
                                return Option.apply(clusterConfig.serverProperties());
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
            ClusterConfig clusterConfig,
            String extendPropertiesMethod) {
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

            @Override
            public ClusterType type() {
                return ClusterType.Quorum;
            }

            @Override
            public ClusterConfig config() {
                return clusterConfig;
            }
        };

        return new TestTemplateInvocationContext() {
            @Override
            public String getDisplayName(int invocationIndex) {
                return "Quorum, " + clusterConfig.ibp().map(ibp -> String.format("IBP=%s, ", ibp)).orElse("") + "Name=" + clusterConfig.name();
            }

            @Override
            public List<Extension> getAdditionalExtensions() {
                return Arrays.asList(
                    (BeforeTestExecutionCallback) context -> {
                        KafkaClusterTestKit.Builder builder = new KafkaClusterTestKit.Builder(
                            new TestKitNodes.Builder().
                                    setNumKip500BrokerNodes(brokers).
                                    setNumControllerNodes(controllers).build());

                        if (extendPropertiesMethod != null && !extendPropertiesMethod.isEmpty()) {
                            extendPerTestProperties(context, extendPropertiesMethod, harness);
                        }
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
                    new ClusterHarnessParameterResolver(harness),
                    new IntegrationTestHelperParameterResolver()
                );
            }
        };
    }
}
