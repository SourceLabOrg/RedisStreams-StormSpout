package org.sourcelab.storm.spout.redis;

import org.sourcelab.storm.spout.redis.failhandler.NoRetryHandler;

import java.io.Serializable;
import java.util.Objects;

/**
 * Configuration properties for the spout.
 */
public class RedisStreamSpoutConfig implements Serializable {
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
     * Prefix name for this consumer. The spout's instance number gets appended to this.
     */
    private final String consumerIdPrefix;

    /**
     * Maximum number of messages to read per consume.
     */
    private final int maxConsumePerRead;

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
     * TupleConverter instance for converting Stream messages into Tuples.
     */
    private final TupleConverter tupleConverter;

    /**
     * FailureHandler instance for handling failures.
     */
    private final FailureHandler failureHandler;

    /**
     * Metric collection enable/disable flag.
     * Defaults to enabled.
     */
    private final boolean metricsEnabled;

    /**
     * Constructor.
     * See Builder instance.
     */
    public RedisStreamSpoutConfig(
        // Redis Connection Properties
        final String host, final int port, final String password,
        // Consumer properties
        final String streamKey, final String groupName, final String consumerIdPrefix,
        // Classes
        final TupleConverter tupleConverterClass, final FailureHandler failureHandlerClass,

        // Other settings
        final int maxConsumePerRead, final int maxTupleQueueSize, final int maxAckQueueSize, final long consumerDelayMillis,
        final boolean metricsEnabled
    ) {
        // Connection Details.
        this.host = Objects.requireNonNull(host);
        this.port = port;
        this.password = password;

        // Consumer Details
        this.groupName = Objects.requireNonNull(groupName);
        this.consumerIdPrefix = Objects.requireNonNull(consumerIdPrefix);
        this.streamKey = Objects.requireNonNull(streamKey);

        // Classes
        this.tupleConverter = Objects.requireNonNull(tupleConverterClass);
        this.failureHandler = Objects.requireNonNull(failureHandlerClass);

        // Other settings
        this.maxConsumePerRead = maxConsumePerRead;
        this.maxTupleQueueSize = maxTupleQueueSize;
        this.maxAckQueueSize = maxAckQueueSize;
        this.consumerDelayMillis = consumerDelayMillis;
        this.metricsEnabled = metricsEnabled;
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

    public String getConsumerIdPrefix() {
        return consumerIdPrefix;
    }

    public int getMaxConsumePerRead() {
        return maxConsumePerRead;
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

    public TupleConverter getTupleConverter() {
        return tupleConverter;
    }

    public FailureHandler getFailureHandler() {
        return failureHandler;
    }

    public boolean isMetricsEnabled() {
        return metricsEnabled;
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
        private String consumerIdPrefix;
        private String streamKey;

        /**
         * Tuple Converter instance.
         */
        private TupleConverter tupleConverter;

        /**
         * Failure Handler instance.
         */
        private FailureHandler failureHandler;

        /**
         * Other configuration properties with sane defaults.
         */
        private int maxConsumePerRead = 512;
        private int maxTupleQueueSize = 1024;
        private int maxAckQueueSize = 1024;
        private long consumerDelayMillis = 1000L;
        private boolean metricsEnabled = true;

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

        public Builder withConsumerIdPrefix(final String consumerIdPrefix) {
            this.consumerIdPrefix = consumerIdPrefix;
            return this;
        }

        public Builder withMaxConsumePerRead(final int limit) {
            this.maxConsumePerRead = limit;
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

        public Builder withTupleConverter(final TupleConverter instance) {
            this.tupleConverter = instance;
            return this;
        }

        public Builder withFailureHandler(final FailureHandler instance) {
            this.failureHandler = instance;
            return this;
        }

        public Builder withNoRetryFailureHandler() {
            this.failureHandler = new NoRetryHandler();
            return this;
        }

        public Builder withMetricsDisabled() {
            return withMetricsEnabled(false);
        }

        public Builder withMetricsEnabled() {
            return withMetricsEnabled(true);
        }

        public Builder withMetricsEnabled(final boolean enabled) {
            this.metricsEnabled = enabled;
            return this;
        }

        /**
         * Creates new Configuration instance.
         * @return Configuration instance.
         */
        public RedisStreamSpoutConfig build() {
            return new RedisStreamSpoutConfig(
                // Redis connection properties
                host, port, password,
                // Consumer Properties
                streamKey, groupName, consumerIdPrefix,
                // Classes
                tupleConverter, failureHandler,
                // Other settings
                maxConsumePerRead, maxTupleQueueSize, maxAckQueueSize, consumerDelayMillis,
                metricsEnabled
            );
        }
    }
}
