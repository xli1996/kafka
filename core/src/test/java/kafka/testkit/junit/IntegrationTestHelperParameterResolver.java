package kafka.testkit.junit;

import integration.kafka.server.IntegrationTestHelper;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;

public class IntegrationTestHelperParameterResolver implements ParameterResolver {
    private static final IntegrationTestHelper TEST_HELPER = new IntegrationTestHelper();

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
        return parameterContext.getParameter().getType().equals(IntegrationTestHelper.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
        return TEST_HELPER;
    }
}
