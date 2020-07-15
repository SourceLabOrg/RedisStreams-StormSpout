package org.sourcelab.storm.spout.redis;

import org.sourcelab.storm.spout.redis.failhandler.NoRetryHandler;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Configuration properties for the spout.
 */
public class RedisStreamSpoutConfig implements Serializable {
    /**
     * Redis server details.
     */
    private final RedisServer redisServer;
    private final RedisCluster redisCluster;

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
     * Use Builder instance.
     */
    private RedisStreamSpoutConfig(
        // Redis Connection Properties
        final RedisServer redisServer,
        final RedisCluster redisCluster,
        // Consumer properties
        final String streamKey, final String groupName, final String consumerIdPrefix,
        // Classes
        final TupleConverter tupleConverterClass, final FailureHandler failureHandlerClass,

        // Other settings
        final int maxConsumePerRead, final int maxTupleQueueSize, final int maxAckQueueSize, final long consumerDelayMillis,
        final boolean metricsEnabled
    ) {
        // Connection
        if (redisCluster != null && redisServer != null) {
            throw new IllegalStateException("TODO");
        } else if (redisCluster == null && redisServer == null) {
            throw new IllegalStateException("TODO");
        }

        this.redisCluster = redisCluster;
        this.redisServer = redisServer;

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

    public boolean isConnectingToCluster() {
        return redisCluster != null;
    }

    /**
     * Build a Redis connection string based on configured properties.
     * @return Redis Connection string.
     */
    public String getConnectString() {
        if (!isConnectingToCluster()) {
            return redisServer.getConnectString();
        }
        return redisCluster.getConnectString();
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
        private final List<RedisServer> clusterServers = new ArrayList<>();
        private RedisServer redisServer = null;

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

        public Builder withServer(final RedisServer redisServer) {
            if (!clusterServers.isEmpty()) {
                // Cannot define both cluster servers and redis server instances.
                throw new IllegalArgumentException("TODO");
            }
            this.redisServer = Objects.requireNonNull(redisServer);
            return this;
        }

        public Builder withServer(final String host, final int port, final String password) {
            return withServer(new RedisServer(host, port, password));
        }

        public Builder withServer(final String host, final int port) {
            return withServer(host, port, null);
        }


        public Builder withClusters(final RedisServer...clusterServers) {
            Arrays.stream(clusterServers)
                .forEach(this::withCluster);
            return this;
        }

        public Builder withCluster(final RedisServer clusterServer) {
            if (redisServer != null) {
                // Cannot define both cluster servers and redis server instances.
                throw new IllegalArgumentException("TODO");
            }
            clusterServers.add(Objects.requireNonNull(clusterServer));
            return this;
        }

        public Builder withCluster(final String host, final int port, final String password) {
            return withCluster(new RedisServer(host, port, password));
        }

        public Builder withCluster(final String host, final int port) {
            return withCluster(host, port, null);
        }

        public Builder withCluster(final RedisCluster redisCluster) {
            Objects.requireNonNull(redisCluster);

            this.clusterServers.clear();
            this.clusterServers.addAll(redisCluster.getServers());
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
            RedisCluster redisCluster = null;
            if (!clusterServers.isEmpty()) {
                redisCluster = new RedisCluster(clusterServers);
            }

            return new RedisStreamSpoutConfig(
                // Redis connection properties
                redisServer, redisCluster,

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

    public static class RedisCluster {
        private final List<RedisServer> servers;

        public RedisCluster(final RedisServer...servers) {
            this(Arrays.asList(servers));
        }

        public RedisCluster(final List<RedisServer> servers) {
            Objects.requireNonNull(servers);
            this.servers = Collections.unmodifiableList(new ArrayList<>(servers));
        }

        public List<RedisServer> getServers() {
            return servers;
        }

        @Override
        public String toString() {
            return "RedisCluster{"
                + "servers=" + servers
                + '}';
        }

        public String getConnectString() {
            return getServers().stream()
                .map(RedisServer::getConnectString)
                .collect(Collectors.joining(","));
        }
    }

    public static class RedisServer {
        private final String host;
        private final int port;
        private final String password;

        public RedisServer(final String host, final int port) {
            this(host, port, null);
        }

        public RedisServer(final String host, final int port, final String password) {
            this.host = host;
            this.port = port;
            this.password = password;
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

        public boolean hasPassword() {
            return password != null;
        }

        public String getConnectString() {
            String connectStr = "redis://";

            if (getPassword() != null && !getPassword().trim().isEmpty()) {
                connectStr += getPassword() + "@";
            }
            connectStr += getHost() + ":" + getPort();

            return connectStr;
        }

        @Override
        public String toString() {
            return "RedisServer{"
                + "host='" + host + '\''
                + ", port=" + port
                + '}';
        }
    }
}
