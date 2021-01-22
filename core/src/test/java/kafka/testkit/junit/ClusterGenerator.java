package kafka.testkit.junit;

import java.util.function.Consumer;

@FunctionalInterface
public interface ClusterGenerator extends Consumer<ClusterConfig> {

}
