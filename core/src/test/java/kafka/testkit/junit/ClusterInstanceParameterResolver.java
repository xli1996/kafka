package kafka.testkit.junit;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.lang.reflect.Executable;

import static org.junit.platform.commons.util.AnnotationUtils.isAnnotated;

/**
 * This resolver provides an instance of {@link ClusterInstance} to a test invocation. The instance represents the
 * underlying cluster being run for the current test. It can be injected into test methods or into the class
 * constructor.
 *
 * N.B., if injected into the class constructor, the instance will not be fully initialized until the actual test method
 * is being invoked. This is because the cluster is not started until after class construction and after "before"
 * lifecycle methods have been run. Constructor injection is meant for convenience so helper methods can be defined on
 * the test which can rely on a class member rather than an argument for ClusterInstance.
 */
public class ClusterInstanceParameterResolver implements ParameterResolver {
    private final ClusterInstance clusterInstance;

    ClusterInstanceParameterResolver(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
        if (!parameterContext.getParameter().getType().equals(ClusterInstance.class)) {
            return false;
        }

        if (!extensionContext.getTestMethod().isPresent()) {
            // Allow this to be injected into the class
           extensionContext.getRequiredTestClass();
           return true;
        } else {
            // If we're injecting into a method, make sure it's a test method and not a lifecycle method
            Executable parameterizedMethod = parameterContext.getParameter().getDeclaringExecutable();
            return isAnnotated(parameterizedMethod, TestTemplate.class);
        }
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
        return clusterInstance;
    }
}
