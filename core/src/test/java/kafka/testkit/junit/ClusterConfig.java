package kafka.testkit.junit;

import kafka.server.KafkaConfig;

import java.util.Optional;
import java.util.Properties;

public class ClusterConfig {
    private final String name;
    private final Properties serverProperties;
    private final String ibp;

    ClusterConfig(Properties serverProperties, String name, String ibp) {
        this.serverProperties = serverProperties;
        this.name = name;
        this.ibp = ibp;
    }

    public Properties serverProperties() {
        return serverProperties;
    }

    public String name() {
        return name;
    }

    public Optional<String> ibp() {
        return Optional.ofNullable(ibp);
    }

    public ClusterConfig copyOf() {
        Properties props = new Properties();
        props.putAll(serverProperties);
        return new ClusterConfig(props, name, ibp);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private Properties serverProperties;
        private String name;
        private String ibp;

        public Builder serverProperties(Properties serverProperties) {
            this.serverProperties = serverProperties;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
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
            return new ClusterConfig(props, name, ibp);
        }
    }
}
