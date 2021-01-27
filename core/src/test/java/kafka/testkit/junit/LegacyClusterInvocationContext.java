/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.testkit.junit;

import integration.kafka.server.IntegrationTestHelper;
import kafka.api.IntegrationTestHarness;
import kafka.network.SocketServer;
import kafka.server.LegacyBroker;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.metadata.BrokerState;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import scala.Option;
import scala.collection.JavaConverters;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Wraps a {@link IntegrationTestHarness} inside lifecycle methods for a test invocation. Each instance of this
 * class is provided with a configuration for the cluster.
 *
 * This context also provides parameter resolvers for:
 *
 * <ul>
 *     <li>ClusterConfig (the same instance passed to the constructor)</li>
 *     <li>ClusterInstance (includes methods to expose underlying SocketServer-s)</li>
 *     <li>IntegrationTestHelper (helper methods)</li>
 * </ul>
 */
public class LegacyClusterInvocationContext implements TestTemplateInvocationContext {

    private final ClusterConfig clusterConfig;
    private final AtomicReference<IntegrationTestHarness> clusterReference;

    public LegacyClusterInvocationContext(ClusterConfig clusterConfig) {
        this.clusterConfig = clusterConfig;
        this.clusterReference = new AtomicReference<>();
    }

    @Override
    public String getDisplayName(int invocationIndex) {
        String clusterDesc = clusterConfig.nameTags().entrySet().stream()
            .map(Object::toString)
            .collect(Collectors.joining(", "));
        return String.format("[Legacy %d] %s", invocationIndex, clusterDesc);
    }

    @Override
    public List<Extension> getAdditionalExtensions() {
        return Arrays.asList(
            (BeforeTestExecutionCallback) context -> {
                // We have to wait to actually create the underlying cluster until after our @BeforeEach methods
                // have run. This allows tests to set up external dependencies like ZK, MiniKDC, etc.
                // However, since we cannot create this instance until we are inside the test invocation, we have
                // to use a container class (AtomicReference) to provide this cluster object to the test itself

                IntegrationTestHarness cluster = new IntegrationTestHarness() { // extends KafkaServerTestHarness
                    @Override
                    public SecurityProtocol securityProtocol() {
                        return SecurityProtocol.forName(clusterConfig.securityProtocol());
                    }

                    @Override
                    public ListenerName listenerName() {
                        return clusterConfig.listenerName().map(ListenerName::normalised)
                            .orElseGet(() -> ListenerName.forSecurityProtocol(securityProtocol()));
                    }

                    @Override
                    public Option<Properties> serverSaslProperties() {
                        return Option.apply(clusterConfig.serverProperties());
                    }

                    @Override
                    public Option<Properties> clientSaslProperties() {
                        // TODO add this to ClusterConfig
                        return super.clientSaslProperties();
                    }

                    @Override
                    public int brokerCount() {
                        // Brokers and controllers are the same in legacy mode, so just use the max
                        return Math.max(clusterConfig.brokers(), clusterConfig.controllers());
                    }
                };
                cluster.adminClientConfig().putAll(clusterConfig.adminClientProperties());
                // TODO consumer and producer configs
                clusterReference.set(cluster);
                clusterReference.get().setUp();

                kafka.utils.TestUtils.waitUntilTrue(
                    () -> clusterReference.get().servers().head().currentState() == BrokerState.RUNNING,
                    () -> "Broker never made it to RUNNING state.",
                    org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS,
                    100L);
            },
            (AfterTestExecutionCallback) context -> clusterReference.get().tearDown(),
            new ClusterInstanceParameterResolver(new LegacyClusterInstance(clusterConfig, clusterReference)),
            new GenericParameterResolver<>(clusterConfig, ClusterConfig.class),
            new GenericParameterResolver<>(new IntegrationTestHelper(), IntegrationTestHelper.class)
        );
    }

    public static class LegacyClusterInstance implements ClusterInstance {

        final AtomicReference<IntegrationTestHarness> clusterReference;
        final ClusterConfig config;

        LegacyClusterInstance(ClusterConfig config, AtomicReference<IntegrationTestHarness> clusterReference) {
            this.config = config;
            this.clusterReference = clusterReference;
        }

        @Override
        public String brokerList() {
            return TestUtils.bootstrapServers(clusterReference.get().servers(), clusterReference.get().listenerName());
        }

        @Override
        public Collection<SocketServer> brokers() {
            return JavaConverters.asJavaCollection(clusterReference.get().servers()).stream()
                    .map(LegacyBroker::socketServer)
                    .collect(Collectors.toList());
        }

        @Override
        public ListenerName listener() {
            return clusterReference.get().listenerName();
        }

        @Override
        public Collection<SocketServer> controllers() {
            return JavaConverters.asJavaCollection(clusterReference.get().servers()).stream()
                .filter(broker -> broker.legacyController().isDefined())
                .map(LegacyBroker::socketServer)
                .collect(Collectors.toList());
    }

        @Override
        public Optional<SocketServer> anyBroker() {
            return JavaConverters.asJavaCollection(clusterReference.get().servers()).stream()
                .filter(broker -> broker.currentState() != BrokerState.NOT_RUNNING && broker.currentState() != BrokerState.SHUTTING_DOWN)
                .map(LegacyBroker::socketServer)
                .findFirst();
        }

        @Override
        public Optional<SocketServer> anyController() {
            return JavaConverters.asJavaCollection(clusterReference.get().servers()).stream()
                .filter(broker -> broker.currentState() != BrokerState.NOT_RUNNING && broker.currentState() != BrokerState.SHUTTING_DOWN)
                .filter(broker -> broker.kafkaController().isActive())
                .map(LegacyBroker::socketServer)
                .findFirst();
        }

        @Override
        public ClusterType clusterType() {
            return ClusterType.Zk;
        }

        @Override
        public ClusterConfig config() {
            return config;
        }

        @Override
        public Object getUnderlying() {
            return clusterReference.get();
        }

        @Override
        public Admin createAdminClient(Properties configOverrides) {
            return clusterReference.get().createAdminClient(configOverrides);
        }
    }
}
