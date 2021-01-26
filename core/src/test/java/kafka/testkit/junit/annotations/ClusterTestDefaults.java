package kafka.testkit.junit.annotations;

import kafka.testkit.junit.ClusterConfig;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Target({TYPE})
@Retention(RUNTIME)
public @interface ClusterTestDefaults {
    ClusterConfig.Type clusterType() default ClusterConfig.Type.Both;
    int brokers() default 1;
    int controllers() default 1;
}
