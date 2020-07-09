package org.sourcelab.storm.spout.redis;

import java.util.Objects;

public class ClientConfiguration {
    /**
     * Redis server details.
     */
    private final String host;
    private final int port;
    private final String password;

    /**
     * The Redis key to stream from.
     */
    private final String streamKey;

    /**
     * Consumer group name.
     */
    private final String groupName;

    /**
     * Name for this consumer.
     */
    private final String consumerId;

    /**
     * Size of the internal buffer for consuming entries from redis.
     */
    private final int maxTupleQueueSize;

    /**
     * Size of the internal buffer for acking entries.
     */
    private final int maxAckQueueSize;

    /**
     * How long should consumer delay between consuming batches.
     */
    private final long consumerDelayMillis;

    private final String tupleConverterClass;


    public ClientConfiguration(
        // Redis Connection Properties
        final String host, final int port, final String password,
        // Consumer properties
        final String streamKey, final String groupName, final String consumerId,
        // Other settings
        final String tupleConverterClass, final int maxTupleQueueSize, final int maxAckQueueSize, final long consumerDelayMillis
    ) {
        // Connection Details.
        this.host = Objects.requireNonNull(host);
        this.port = port;
        this.password = password;

        // Consumer Details
        this.groupName = Objects.requireNonNull(groupName);
        this.consumerId = Objects.requireNonNull(consumerId);
        this.streamKey = Objects.requireNonNull(streamKey);

        // Other settings
        this.tupleConverterClass = Objects.requireNonNull(tupleConverterClass);
        this.maxTupleQueueSize = maxTupleQueueSize;
        this.maxAckQueueSize = maxAckQueueSize;
        this.consumerDelayMillis = consumerDelayMillis;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getPassword() {
        return password;
    }

    public String getStreamKey() {
        return streamKey;
    }

    public String getGroupName() {
        return groupName;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public String getConnectString() {
        String connectStr = "redis://";
        if (getPassword() != null && !getPassword().trim().isEmpty()) {
            connectStr += getPassword() + "@";
        }
        connectStr += getHost() + ":" + getPort();

        return connectStr;
    }

    public int getMaxTupleQueueSize() {
        return maxTupleQueueSize;
    }

    public int getMaxAckQueueSize() {
        return maxAckQueueSize;
    }

    public long getConsumerDelayMillis() {
        return consumerDelayMillis;
    }

    public String getTupleConverterClass() {
        return tupleConverterClass;
    }

    public static Builder newBuilder() {
        return new Builder();
    }


    public static final class Builder {
        /**
         * Connection details.
         */
        private String host;
        private int port;
        private String password;

        /**
         * Consumer details.
         */
        private String groupName;
        private String consumerId;
        private String streamKey;

        /**
         * Other configuration proprties with sane defaults.
         */
        private String tupleConverterClass;
        private int maxTupleQueueSize = 1024;
        private int maxAckQueueSize = 1024;
        private long consumerDelayMillis = 1000L;

        private Builder() {
        }

        public Builder withHost(String host) {
            this.host = host;
            return this;
        }

        public Builder withPort(Object port) {
            Objects.requireNonNull(port);
            if (port instanceof Number) {
                return withPort(((Number) port).intValue());
            } else if (port instanceof String) {
                return withPort(Integer.parseInt((String) port));
            }
            throw new IllegalArgumentException("Port must be a Number!");
        }

        public Builder withPort(int port) {
            this.port = port;
            return this;
        }

        public Builder withPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder withStreamKey(String key) {
            this.streamKey = key;
            return this;
        }

        public Builder withGroupName(String groupName) {
            this.groupName = groupName;
            return this;
        }

        public Builder withConsumerId(String consumerId) {
            this.consumerId = consumerId;
            return this;
        }

        public Builder withMaxTupleQueueSize(int limit) {
            this.maxTupleQueueSize = limit;
            return this;
        }

        public Builder withMaxAckQueueSize(int limit) {
            this.maxAckQueueSize = limit;
            return this;
        }

        public Builder withConsumerDelayMillis(final long millis) {
            this.consumerDelayMillis = millis;
            return this;
        }

        public Builder withTupleConverterClass(final String classStr) {
            this.tupleConverterClass = classStr;
            return this;
        }

        public ClientConfiguration build() {
            return new ClientConfiguration(
                // Redis connection properties
                host, port, password,
                // Consumer Properties
                streamKey, groupName, consumerId,
                // Other settings
                tupleConverterClass, maxTupleQueueSize, maxAckQueueSize, consumerDelayMillis
            );
        }

    }
}
