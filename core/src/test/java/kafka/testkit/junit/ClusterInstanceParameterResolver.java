package kafka.testkit.junit;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;


public class ClusterInstanceParameterResolver implements ParameterResolver {
    private final ClusterInstance clusterInstance;

    ClusterInstanceParameterResolver(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
        return parameterContext.getParameter().getType().equals(ClusterInstance.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
        return clusterInstance;
    }
}
