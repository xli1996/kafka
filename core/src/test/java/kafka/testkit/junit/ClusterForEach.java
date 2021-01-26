package kafka.testkit.junit;

import kafka.testkit.junit.annotations.ClusterProperty;
import kafka.testkit.junit.annotations.ClusterTemplate;
import kafka.testkit.junit.annotations.ClusterTest;
import kafka.testkit.junit.annotations.ClusterTests;
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
 * ClusterForEach is a custom JUnit extension that will generate some number of test invocations depending on a few
 * custom annotations. These annotations are placed on so-called test template methods. Template methods look like
 * normal JUnit test methods, but instead of being invoked directly, they are used as templates for generating
 * multiple test invocations.
 *
 * Test class that use this extension should use one of the following annotations on each template method:
 *
 * <ul>
 *     <li>{@link ClusterTest}, define a single cluster configuration</li>
 *     <li>{@link ClusterTests}, provide multiple instances of @ClusterTest</li>
 *     <li>{@link ClusterTemplate}, define a static method that generates cluster configurations</li>
 * </ul>
 *
 * Any combination of these annotations may be used on a given test template method. If no test invocations are
 * generated after processing the annotations, an error is thrown.
 *
 * Depending on which annotations are used, and what values are given, different {@link ClusterConfig} will be
 * generated. Each ClusterConfig is used to create an underlying Kafka cluster that is used for the actual test
 * invocation.
 *
 * For example:
 *
 * <pre>
 * &#64;ExtendWith(value = Array(classOf[ClusterForEach]))
 * class SomeIntegrationTest {
 *   &#64;ClusterTest(brokers = 1, controllers = 1, clusterType = ClusterType.Both)
 *   def someTest(): Unit = {
 *     assertTrue(condition)
 *   }
 * }
 * </pre>
 *
 * will generate two invocations of "someTest" (since ClusterType.Both was given). For each invocation, the test class
 * SomeIntegrationTest will be instantiated, lifecycle methods (before/after) will be run, and "someTest" will be invoked.
 *
 **/
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
            processClusterTest(context, clusterTestAnnot, generatedContexts::add);
        }

        // Process multiple @ClusterTest annotation within @ClusterTests
        ClusterTests clusterTestsAnnot = context.getRequiredTestMethod().getDeclaredAnnotation(ClusterTests.class);
        if (clusterTestsAnnot != null) {
            for (ClusterTest annot : clusterTestsAnnot.value()) {
                processClusterTest(context, annot, generatedContexts::add);
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

    private void processClusterTest(ExtensionContext context, ClusterTest annot, Consumer<TestTemplateInvocationContext> testInvocations) {
        ClusterConfig.Builder builder = ClusterConfig.clusterBuilder(
            annot.clusterType(), annot.brokers(), annot.controllers(), annot.securityProtocol());
        if (!annot.name().isEmpty()) {
            builder.name(annot.name());
        }
        if (!annot.listener().isEmpty()) {
            builder.listenerName(annot.listener());
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
