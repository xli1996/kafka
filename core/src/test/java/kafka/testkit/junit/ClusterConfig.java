package kafka.testkit.junit;

import kafka.server.KafkaConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/**
 * Represents a requested configuration of a Kafka cluster for integration testing
 */
public class ClusterConfig {
    public enum Type {
        Quorum,
        Legacy,
        Both
    }

    private final Type type;
    private final int brokers;
    private final int controllers;
    private final String name;
    private final Properties serverProperties;
    private final String ibp;
    private final String securityProtocol;
    private final String listenerName;

    ClusterConfig(Type type, int brokers, int controllers, String name, Properties serverProperties, String ibp,
                  String securityProtocol, String listenerName) {
        this.type = type;
        this.brokers = brokers;
        this.controllers = controllers;
        this.name = name;
        this.serverProperties = serverProperties;
        this.ibp = ibp;
        this.securityProtocol = securityProtocol;
        this.listenerName = listenerName;
    }

    public Type clusterType() {
        return type;
    }

    public int brokers() {
        return brokers;
    }

    public int controllers() {
        return controllers;
    }

    public Optional<String> name() {
        return Optional.ofNullable(name);
    }

    public Properties serverProperties() {
        return serverProperties;
    }

    public Optional<String> ibp() {
        return Optional.ofNullable(ibp);
    }

    public String securityProtocol() {
        return securityProtocol;
    }

    public Optional<String> listenerName() {
        return Optional.ofNullable(listenerName);
    }

    public Map<String, String> nameTags() {
        Map<String, String> tags = new LinkedHashMap<>(3);
        name().ifPresent(name -> tags.put("Name", name));
        ibp().ifPresent(ibp -> tags.put("IBP", ibp));
        tags.put("security", securityProtocol);
        listenerName().ifPresent(listener -> tags.put("listener", listener));
        return tags;
    }

    public ClusterConfig copyOf() {
        Properties props = new Properties();
        props.putAll(serverProperties);
        return new ClusterConfig(type, brokers, controllers, name, props, ibp, securityProtocol, listenerName);
    }

    public static Builder defaultClusterBuilder() {
        return new Builder(Type.Quorum, 1, 1, SecurityProtocol.PLAINTEXT.name);
    }

    public static Builder clusterBuilder(Type type, int brokers, int controllers, String securityProtocol) {
        return new Builder(type, brokers, controllers, securityProtocol);
    }

    public static class Builder {
        private Type type;
        private int brokers;
        private int controllers;
        private String name;
        private Properties serverProperties;
        private String ibp;
        private String securityProtocol;
        private String listenerName;

        Builder(Type type, int brokers, int controllers, String securityProtocol) {
            this.type = type;
            this.brokers = brokers;
            this.controllers = controllers;
            this.securityProtocol = securityProtocol;
        }

        public Builder type(Type type) {
            this.type = type;
            return this;
        }

        public Builder brokers(int brokers) {
            this.brokers = brokers;
            return this;
        }

        public Builder controllers(int controllers) {
            this.controllers = controllers;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder serverProperties(Properties serverProperties) {
            this.serverProperties = serverProperties;
            return this;
        }

        public Builder ibp(String interBrokerProtocol) {
            this.ibp = interBrokerProtocol;
            return this;
        }

        public Builder securityProtocol(String securityProtocol) {
            this.securityProtocol = securityProtocol;
            return this;
        }

        public Builder listenerName(String listenerName) {
            this.listenerName = listenerName;
            return this;
        }

        public ClusterConfig build() {
            Properties props = new Properties();
            if (serverProperties != null) {
                props.putAll(serverProperties);
            }
            if (ibp != null) {
                props.put(KafkaConfig.InterBrokerProtocolVersionProp(), ibp);
            }
            return new ClusterConfig(type, brokers, controllers, name, props, ibp, securityProtocol, listenerName);
        }
    }
}
