package kafka.testkit;

import kafka.network.SocketServer;
import org.apache.kafka.common.network.ListenerName;

import java.util.Collection;

public interface ClusterHarness {

    Collection<SocketServer> brokers();

    ListenerName listener();

    Collection<SocketServer> controllers();
}
