package kafka.testkit.junit.annotations;

import kafka.testkit.junit.ClusterConfig;
import org.junit.jupiter.api.TestTemplate;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Documented
@Target({METHOD})
@Retention(RUNTIME)
@TestTemplate
public @interface ClusterTest {
    ClusterConfig.Type clusterType() default ClusterConfig.Type.Default;
    int brokers() default 0;
    int controllers() default 0;

    String name() default "";
    String securityProtocol() default "PLAINTEXT";
    String listener() default "";
    ClusterProperty[] properties() default {};
}


