package kafka.testkit.junit;

import kafka.network.SocketServer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.network.ListenerName;

import java.util.Collection;
import java.util.Optional;
import java.util.Properties;

public interface ClusterInstance {

    enum ClusterType {
        Zk,
        Raft
    }

    ClusterType clusterType();

    ClusterConfig config();

    ListenerName listener();

    String brokerList();

    Collection<SocketServer> brokers();

    Collection<SocketServer> controllers();

    Optional<SocketServer> anyBroker();

    Optional<SocketServer> anyController();

    Object getUnderlying();

    default <T> T getUnderlying(Class<T> asClass) {
        return asClass.cast(getUnderlying());
    }

    Admin createAdminClient(Properties configOverrides);

    default Admin createAdminClient() {
        return createAdminClient(new Properties());
    }

}
