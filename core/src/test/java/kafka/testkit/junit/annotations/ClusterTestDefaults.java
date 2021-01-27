package kafka.testkit.junit.annotations;

import kafka.testkit.junit.ClusterConfig;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Used to set class level defaults for any test template methods annotated with {@link ClusterTest} or
 * {@link ClusterTests}. The default values here are also used as the source for defaults in
 * {@link kafka.testkit.junit.ClusterForEach}.
 */
@Documented
@Target({TYPE})
@Retention(RUNTIME)
public @interface ClusterTestDefaults {
    ClusterConfig.Type clusterType() default ClusterConfig.Type.Zk;
    int brokers() default 1;
    int controllers() default 1;
}
