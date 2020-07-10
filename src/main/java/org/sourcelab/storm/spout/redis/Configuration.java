package org.sourcelab.storm.spout.redis;

import java.util.Objects;

/**
 * Configuration properties for the spout.
 */
public class Configuration {
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

    /**
     * Name of the class to use for converting Stream messages into Tuples.
     */
    private final String tupleConverterClass;

    /**
     * Name of the class to use for handling failures.
     */
    private final String failureHandlerClass;

    /**
     * Constructor.
     * See Builder instance.
     */
    public Configuration(
        // Redis Connection Properties
        final String host, final int port, final String password,
        // Consumer properties
        final String streamKey, final String groupName, final String consumerId,
        // Classes
        final String tupleConverterClass, final String failureHandlerClass,

        // Other settings
        final int maxTupleQueueSize, final int maxAckQueueSize, final long consumerDelayMillis
    ) {
        // Connection Details.
        this.host = Objects.requireNonNull(host);
        this.port = port;
        this.password = password;

        // Consumer Details
        this.groupName = Objects.requireNonNull(groupName);
        this.consumerId = Objects.requireNonNull(consumerId);
        this.streamKey = Objects.requireNonNull(streamKey);

        // Classes
        this.tupleConverterClass = Objects.requireNonNull(tupleConverterClass);
        this.failureHandlerClass = Objects.requireNonNull(failureHandlerClass);

        // Other settings
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

    /**
     * Build a Redis connection string based on configured properties.
     * @return Redis Connection string.
     */
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

    public String getFailureHandlerClass() {
        return failureHandlerClass;
    }

    /**
     * Create a new Builder instance.
     * @return Builder for Configuration instance.
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for Configuration instance.
     */
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
         * Tuple Converter class.
         */
        private String tupleConverterClass;

        /**
         * Failure Handler class.
         */
        private String failureHandlerClass;

        /**
         * Other configuration properties with sane defaults.
         */
        private int maxTupleQueueSize = 1024;
        private int maxAckQueueSize = 1024;
        private long consumerDelayMillis = 1000L;

        private Builder() {
        }

        public Builder withHost(final String host) {
            this.host = host;
            return this;
        }

        /**
         * Set the port parameter.  Attempts to handle input in both
         * Number or String input.
         *
         * @param port Port value.
         * @return Builder instance.
         * @throws IllegalArgumentException if passed a non-number representation value.
         */
        public Builder withPort(final Object port) {
            Objects.requireNonNull(port);
            if (port instanceof Number) {
                return withPort(((Number) port).intValue());
            } else if (port instanceof String) {
                return withPort(Integer.parseInt((String) port));
            }
            throw new IllegalArgumentException("Port must be a Number!");
        }

        public Builder withPort(final int port) {
            this.port = port;
            return this;
        }

        public Builder withPassword(final String password) {
            this.password = password;
            return this;
        }

        public Builder withStreamKey(final String key) {
            this.streamKey = key;
            return this;
        }

        public Builder withGroupName(final String groupName) {
            this.groupName = groupName;
            return this;
        }

        public Builder withConsumerId(final String consumerId) {
            this.consumerId = consumerId;
            return this;
        }

        public Builder withMaxTupleQueueSize(final int limit) {
            this.maxTupleQueueSize = limit;
            return this;
        }

        public Builder withMaxAckQueueSize(final int limit) {
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

        public Builder withFailureHandlerClass(final String classStr) {
            this.failureHandlerClass = classStr;
            return this;
        }

        /**
         * Creates new Configuration instance.
         * @return Configuration instance.
         */
        public Configuration build() {
            return new Configuration(
                // Redis connection properties
                host, port, password,
                // Consumer Properties
                streamKey, groupName, consumerId,
                // Classes
                tupleConverterClass, failureHandlerClass,
                // Other settings
                maxTupleQueueSize, maxAckQueueSize, consumerDelayMillis
            );
        }
    }
}
