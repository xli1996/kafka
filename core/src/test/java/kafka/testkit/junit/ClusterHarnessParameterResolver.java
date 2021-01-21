package kafka.testkit.junit;

import kafka.testkit.ClusterHarness;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;


public class ClusterHarnessParameterResolver implements ParameterResolver {
    private final ClusterHarness clusterHarness;

    ClusterHarnessParameterResolver(ClusterHarness clusterHarness) {
        this.clusterHarness = clusterHarness;
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
        return parameterContext.getParameter().getType().equals(ClusterHarness.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
        return clusterHarness;
    }
}
