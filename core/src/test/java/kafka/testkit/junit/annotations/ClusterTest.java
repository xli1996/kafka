package kafka.testkit.junit.annotations;

import kafka.testkit.junit.ClusterConfig;
import org.junit.jupiter.api.TestTemplate;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Target({TYPE,METHOD})
@Retention(RUNTIME)
@TestTemplate
public @interface ClusterTest {
    ClusterConfig.Type clusterType() default ClusterConfig.Type.Both;
    String name() default "";
    int brokers() default 1;
    int controllers() default 1;
    String securityProtocol() default "PLAINTEXT";
    String listener() default "";
    ClusterProperty[] properties() default {};
}


