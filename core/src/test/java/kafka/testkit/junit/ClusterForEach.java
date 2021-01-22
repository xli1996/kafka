package kafka.testkit.junit;

import kafka.testkit.junit.annotations.ClusterProperty;
import kafka.testkit.junit.annotations.ClusterTemplate;
import kafka.testkit.junit.annotations.ClusterTest;
import kafka.testkit.junit.annotations.ClusterTests;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.junit.platform.commons.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;
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
 *
 * Test templates must be annotated with {@link ClusterTemplate} in order to be discovered. The fields from the annonation
 * will determine the number of invocations that are generated as well as the configuration for the clusters used by
 * each invocation.
 */
public class ClusterForEach implements TestTemplateInvocationContextProvider {
    @Override
    public boolean supportsTestTemplate(ExtensionContext context) {
        return true;
    }

    private void generateClusterConfigurations(ExtensionContext context, String generateClustersMethods, ClusterGenerator generator) {
        Object testInstance = context.getTestInstance().orElse(null);
        Method method = ReflectionUtils.getRequiredMethod(context.getRequiredTestClass(), generateClustersMethods, ClusterGenerator.class);
        ReflectionUtils.invokeMethod(method, testInstance, generator);
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context) {
        List<TestTemplateInvocationContext> generatedContexts = new ArrayList<>();

        // Process the @ClusterTemplate annotation
        ClusterTemplate clusterTemplateAnnot = context.getRequiredTestMethod().getDeclaredAnnotation(ClusterTemplate.class);
        if (clusterTemplateAnnot != null) {
            processClusterTemplate(context, clusterTemplateAnnot, generatedContexts::add);
            if (generatedContexts.size() == 0) {
                throw new IllegalStateException("ClusterConfig generator method should provide at least one config");
            }
        }

        // Process single @ClusterTest annotation
        ClusterTest clusterTestAnnot = context.getRequiredTestMethod().getDeclaredAnnotation(ClusterTest.class);
        if (clusterTestAnnot != null) {
            processClusterConfig(context, clusterTestAnnot, generatedContexts::add);
        }

        // Process multiple @ClusterTest annotation within @ClusterTests
        ClusterTests clusterTestsAnnot = context.getRequiredTestMethod().getDeclaredAnnotation(ClusterTests.class);
        if (clusterTestsAnnot != null) {
            for (ClusterTest annot : clusterTestsAnnot.value()) {
                processClusterConfig(context, annot, generatedContexts::add);
            }
        }

        if (generatedContexts.size() == 0) {
            throw new IllegalStateException("Please annotate test methods with @ClusterTemplate, @ClusterTest, or @ClusterTests when using the ClusterForEach provider");
        }

        return generatedContexts.stream();
    }

    private void processClusterTemplate(ExtensionContext context, ClusterTemplate annot, Consumer<TestTemplateInvocationContext> testInvocations) {
        // If specified, call cluster config generated method (must be static)
        List<ClusterConfig> generatedClusterConfigs = new ArrayList<>();
        if (!annot.value().isEmpty()) {
            generateClusterConfigurations(context, annot.value(), generatedClusterConfigs::add);
        } else {
            // Ensure we have at least one cluster config
            generatedClusterConfigs.add(ClusterConfig.defaultClusterBuilder().build());
        }

        generatedClusterConfigs.forEach(config -> {
            switch(config.clusterType()) {
                case Quorum:
                    testInvocations.accept(new QuorumClusterInvocationContext(config.copyOf()));
                    break;
                case Legacy:
                    testInvocations.accept(new LegacyClusterInvocationContext(config.copyOf()));
                    break;
                default:
                    throw new IllegalStateException("Unknown cluster type " + config.clusterType());
            }
        });

    }

    private void processClusterConfig(ExtensionContext context, ClusterTest annot, Consumer<TestTemplateInvocationContext> testInvocations) {
        // TODO these
        SecurityProtocol securityProtocol = SecurityProtocol.forName(annot.securityProtocol());
        ListenerName listenerName;
        if (annot.listener().isEmpty()) {
            listenerName = ListenerName.forSecurityProtocol(securityProtocol);
        } else {
            listenerName = ListenerName.normalised(annot.listener());
        }

        ClusterConfig.Builder builder = ClusterConfig.clusterBuilder(annot.clusterType(), annot.brokers(), annot.controllers());
        if (!annot.name().isEmpty()) {
            builder.name(annot.name());
        }
        Properties properties = new Properties();
        for (ClusterProperty property : annot.properties()) {
            properties.put(property.key(), property.value());
        }
        builder.serverProperties(properties);
        if (annot.clusterType().equals(ClusterConfig.ClusterType.Legacy)) {
            testInvocations.accept(new LegacyClusterInvocationContext(builder.build()));
        } else if (annot.clusterType().equals(ClusterConfig.ClusterType.Quorum)) {
            testInvocations.accept(new QuorumClusterInvocationContext(builder.build()));
        } else {
            testInvocations.accept(new LegacyClusterInvocationContext(builder.build()));
            testInvocations.accept(new QuorumClusterInvocationContext(builder.build()));
        }
    }
}
