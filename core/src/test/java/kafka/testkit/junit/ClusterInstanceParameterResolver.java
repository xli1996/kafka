package kafka.testkit.junit;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.lang.reflect.Executable;

import static org.junit.platform.commons.util.AnnotationUtils.isAnnotated;

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
