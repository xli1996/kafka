package kafka.testkit.junit;

import org.junit.jupiter.api.TestTemplate;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Target({TYPE,METHOD})
@Retention(RUNTIME)
@TestTemplate
public @interface ClusterTemplate {
    int brokers() default 1;
    int controllers() default 1;
    String securityProtocol() default "PLAINTEXT";
    String listener() default "";
    String generateClusters() default "";
}
