package kafka.testkit.junit;

import kafka.testkit.junit.annotations.ClusterProperty;
import kafka.testkit.junit.annotations.ClusterTemplate;
import kafka.testkit.junit.annotations.ClusterTest;
import kafka.testkit.junit.annotations.ClusterTestDefaults;
import kafka.testkit.junit.annotations.ClusterTests;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;


@ClusterTestDefaults(clusterType = ClusterConfig.Type.Zk)   // Set defaults for a few params in @ClusterTest(s)
@ExtendWith(ClusterForEach.class)
public class ClusterForEachTest {

    private final ClusterInstance clusterInstance;
    private final ClusterConfig config;

    ClusterForEachTest(ClusterInstance clusterInstance, ClusterConfig config) {     // Constructor injections
        this.clusterInstance = clusterInstance;
        this.config = config;
    }

    // Static methods can generate cluster configurations
    static void generate1(ClusterGenerator clusterGenerator) {
        clusterGenerator.accept(ClusterConfig.defaultClusterBuilder().type(ClusterConfig.Type.Raft).build());
    }

    // BeforeEach run after class construction, but before cluster initialization and test invocation
    @BeforeEach
    public void beforeEach(ClusterConfig config) {
        Assertions.assertSame(this.config, config, "Injected objects should be the same");
        config.serverProperties().put("before", "each");
    }

    // AfterEach runs after test invocation and cluster teardown
    @AfterEach
    public void afterEach(ClusterConfig config) {
        Assertions.assertSame(this.config, config, "Injected objects should be the same");
    }

    // With no params, configuration comes from the annotation defaults as well as @ClusterTestDefaults (if present)
    @ClusterTest
    public void testClusterTest(ClusterConfig config, ClusterInstance clusterInstance) {
        Assertions.assertSame(this.config, config, "Injected objects should be the same");
        Assertions.assertSame(this.clusterInstance, clusterInstance, "Injected objects should be the same");
        Assertions.assertEquals(clusterInstance.clusterType(), ClusterInstance.ClusterType.Zk); // From the class level default
        Assertions.assertEquals(clusterInstance.config().serverProperties().getProperty("before"), "each");
    }

    // generate1 is a template method which generates any number of cluster configs
    @ClusterTemplate("generate1")
    public void testClusterTemplate() {
        Assertions.assertEquals(clusterInstance.clusterType(), ClusterInstance.ClusterType.Raft,
            "generate1 provided a Raft cluster, so we should see that here");
        Assertions.assertEquals(clusterInstance.config().serverProperties().getProperty("before"), "each");
    }

    // Multiple @ClusterTest can be used with @ClusterTests
    @ClusterTests({
        @ClusterTest(clusterType = ClusterConfig.Type.Zk, properties = {
            @ClusterProperty(key = "foo", value = "bar"),
            @ClusterProperty(key = "spam", value = "eggs")
        }),
        @ClusterTest(clusterType = ClusterConfig.Type.Raft, properties = {
            @ClusterProperty(key = "foo", value = "baz"),
            @ClusterProperty(key = "spam", value = "eggz")
        })
    })
    public void testClusterTests() {
        if (clusterInstance.clusterType().equals(ClusterInstance.ClusterType.Zk)) {
            Assertions.assertEquals(clusterInstance.config().serverProperties().getProperty("foo"), "bar");
            Assertions.assertEquals(clusterInstance.config().serverProperties().getProperty("spam"), "eggs");
        } else if (clusterInstance.clusterType().equals(ClusterInstance.ClusterType.Raft)) {
            Assertions.assertEquals(clusterInstance.config().serverProperties().getProperty("foo"), "baz");
            Assertions.assertEquals(clusterInstance.config().serverProperties().getProperty("spam"), "eggz");
        } else {
            Assertions.fail("Unknown cluster type " + clusterInstance.clusterType());
        }
    }
}
