package kafka.testkit.junit.annotations;

import org.junit.jupiter.api.TestTemplate;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Used to indicate that a test should call the method given by {@link #value()} to generate a number of
 * cluster configurations. The method specified by the value should accept a single argument of the type
 * {@link kafka.testkit.junit.ClusterGenerator}. Any return value from the method is ignore. A test invocation
 * will be generated for each {@link kafka.testkit.junit.ClusterConfig} provided to the ClusterGenerator instance.
 *
 * The method given here must be static since it is invoked before any tests are actually run. Each test generated
 * by this annotation will run as if it was defined as a separate test method with its own
 * {@link org.junit.jupiter.api.Test}. That is to say, each generated test invocation will have a separate lifecycle.
 *
 * This annotation may be used in conjunction with {@link ClusterTest} and {@link ClusterTests} which also yield
 * ClusterConfig instances.
 *
 * For Scala tests, the method should be defined in a companion object with the same name as the test class.
 */
@Target({METHOD})
@Retention(RUNTIME)
@TestTemplate
public @interface ClusterTemplate {
    String value();
}
