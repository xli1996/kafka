package kafka.testkit.junit;

import kafka.network.SocketServer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.network.ListenerName;

import java.util.Collection;
import java.util.Optional;
import java.util.Properties;

public interface ClusterInstance {

    String brokerList();

    Collection<SocketServer> brokers();

    ListenerName listener();

    Collection<SocketServer> controllers();

    Optional<SocketServer> anyBroker();

    Optional<SocketServer> anyController();

    ClusterType clusterType();

    ClusterConfig config();

    Object getUnderlying();

    default Admin createAdminClient() {
        return createAdminClient(new Properties());
    }

    Admin createAdminClient(Properties configOverrides);

    enum ClusterType {
        Zk,
        Raft
    }
}
