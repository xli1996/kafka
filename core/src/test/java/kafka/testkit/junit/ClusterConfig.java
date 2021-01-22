package kafka.testkit.junit;

import kafka.server.KafkaConfig;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/**
 * Represents a requested configuration of a Kafka cluster for integration testing
 */
public class ClusterConfig {
    public enum ClusterType {
        Quorum,
        Legacy,
        Both
    }

    private final ClusterType type;
    private final int brokers;
    private final int controllers;
    private final String name;
    private final Properties serverProperties;
    private final String ibp;

    ClusterConfig(ClusterType type, int brokers, int controllers, String name, Properties serverProperties, String ibp) {
        this.type = type;
        this.brokers = brokers;
        this.controllers = controllers;
        this.name = name;
        this.serverProperties = serverProperties;
        this.ibp = ibp;
    }

    public ClusterType clusterType() {
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

    public Map<String, String> nameTags() {
        Map<String, String> tags = new LinkedHashMap<>(3);
        name().ifPresent(name -> tags.put("Name", name));
        ibp().ifPresent(ibp -> tags.put("IBP", ibp));
        return tags;
    }

    public ClusterConfig copyOf() {
        Properties props = new Properties();
        props.putAll(serverProperties);
        return new ClusterConfig(type, brokers, controllers, name, props, ibp);
    }

    public static Builder defaultClusterBuilder() {
        return new Builder(ClusterType.Quorum, 1, 1);
    }

    public static Builder clusterBuilder(ClusterType type, int brokers, int controllers) {
        return new Builder(type, brokers, controllers);
    }

    public static class Builder {
        private ClusterConfig.ClusterType type;
        private int brokers;
        private int controllers;
        private String name;
        private Properties serverProperties;
        private String ibp;

        Builder(ClusterConfig.ClusterType type, int brokers, int controllers) {
            this.type = type;
            this.brokers = brokers;
            this.controllers = controllers;
        }

        public Builder type(ClusterConfig.ClusterType type) {
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

        public ClusterConfig build() {
            Properties props = new Properties();
            if (serverProperties != null) {
                props.putAll(serverProperties);
            }
            if (ibp != null) {
                props.put(KafkaConfig.InterBrokerProtocolVersionProp(), ibp);
            }
            return new ClusterConfig(type, brokers, controllers, name, props, ibp);
        }
    }
}
