package kafka.testkit.junit;

import kafka.network.SocketServer;
import org.apache.kafka.common.network.ListenerName;

import java.util.Collection;

public interface ClusterInstance {

    Collection<SocketServer> brokers();

    ListenerName listener();

    Collection<SocketServer> controllers();

    ClusterType clusterType();

    ClusterConfig config();

    enum ClusterType {
        Legacy,
        Quorum
    }
}
