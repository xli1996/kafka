package kafka.testkit.junit;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;

/**
 * This resolver is used for supplying any type of object to the test invocation. It does not restrict where the given
 * type can be injected, it simply checks if the requested injection type matches the type given in the constructor. If
 * it matches, the given object is returned.
 *
 * This is useful for injecting helper objects and objects which can be fully initialized before the test lifecycle
 * begins.
 */
public class GenericParameterResolver<T> implements ParameterResolver {

    private final T instance;
    private final Class<T> clazz;

    GenericParameterResolver(T instance, Class<T> clazz) {
        this.instance = instance;
        this.clazz = clazz;
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
        return parameterContext.getParameter().getType().equals(clazz);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
        return instance;
    }
}
