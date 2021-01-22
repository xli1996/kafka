package kafka.testkit.junit.annotations;

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
    String value();
}
