package kafka.testkit.junit;

import kafka.network.SocketServer;
import org.apache.kafka.common.network.ListenerName;

import java.util.Collection;

public interface ClusterInstance {

    Collection<SocketServer> brokers();

    ListenerName listener();

    Collection<SocketServer> controllers();

    ClusterType type();

    ClusterConfig config();

    enum ClusterType {
        Legacy,
        Quorum
    }
}
