package org.sourcelab.storm.spout.redis.util;

import org.apache.storm.task.TopologyContext;
import org.sourcelab.storm.spout.redis.Configuration;

import java.util.Map;
import java.util.Objects;

/**
 * Utility class for building ClientConfiguration instances from Storm config maps.
 */
public class StormToClientConfigurationUtil {
    // Connection Details
    public static final String REDIS_SERVER_HOST = "redis_stream_spout.server.host";
    public static final String REDIS_SERVER_PORT = "redis_stream_spout.server.port";
    public static final String REDIS_SERVER_PASSWORD = "redis_stream_spout.server.password";

    // Consumer Details
    public static final String REDIS_CONSUMER_GROUP_NAME = "redis_stream_spout.consumer.group_name";
    public static final String REDIS_CONSUMER_CONSUMER_ID_PREFIX = "redis_stream_spout.consumer.consumer_id_prefix";
    public static final String REDIS_CONSUMER_STREAM_KEY = "redis_stream_spout.consumer.stream_key";

    // Tuple Conversion
    public static final String TUPLE_CONVERTER_CLASS = "redis_stream_spout.tuple_converter_class";

    // Other optional settings
    public static final String CONSUMER_DELAY_MILLIS = "redis_stream_spout.consumer.delay_millis";
    public static final String CONSUMER_MAX_TUPLE_QUEUE_SIZE = "redis_stream_spout.consumer.max_tuple_queue_size";
    public static final String CONSUMER_MAX_ACK_QUEUE_SIZE = "redis_stream_spout.consumer.max_ack_queue_size";

    // Retry Failure Handler settings
    public static final String FAILURE_HANDLER_MAX_RETRIES = "redis_streams_spout.failure.max_retries";

    /**
     * Required properties.
     */
    public static final String[] REQUIRED_CONFIGURATION_KEYS = new String[] {
        // Server keys
        REDIS_SERVER_HOST, REDIS_SERVER_PORT,

        // Consumer keys
        REDIS_CONSUMER_GROUP_NAME, REDIS_CONSUMER_STREAM_KEY, REDIS_CONSUMER_CONSUMER_ID_PREFIX,

        // Other
        TUPLE_CONVERTER_CLASS
    };

    /**
     * Utility method to take a Storm Spout Configuration map and convert it into a ClientConfiguration instance.
     * @param stormConfig Spout config provided by Storm.
     * @param topologyContext TopologyContext provided by Storm.
     * @return ClientConfiguration instance configured.
     */
    public static Configuration load(final Map<String, Object> stormConfig, final TopologyContext topologyContext) {
        // Validate inputs.
        Objects.requireNonNull(stormConfig);
        Objects.requireNonNull(topologyContext);

        // Validate required keys exist.
        validateRequiredKeys(stormConfig);

        // Create builder and populate it.
        final Configuration.Builder builder = Configuration.newBuilder();
        loadTupleConverter(builder, stormConfig);
        loadServerSettings(builder, stormConfig);
        loadConsumerSettings(builder, stormConfig, topologyContext);
        loadOptionalSettings(builder, stormConfig);

        // Build instance.
        return builder.build();
    }

    private static void loadTupleConverter(final Configuration.Builder builder, final Map<String, Object> stormConfig) {
        String classStr = (String) stormConfig.get(TUPLE_CONVERTER_CLASS);
        if (classStr == null || classStr.trim().isEmpty()) {
            throw new IllegalStateException("Invalid value for key '" + TUPLE_CONVERTER_CLASS + "'");
        }
        builder.withTupleConverterClass(classStr);
    }

    private static void validateRequiredKeys(final Map<String, Object> stormConfig) {
        for (final String requiredKey : REQUIRED_CONFIGURATION_KEYS) {
            if (!stormConfig.containsKey(requiredKey)) {
                throw new IllegalStateException("Missing required configuration key '" + requiredKey + "'");
            }
            if (stormConfig.get(requiredKey) == null) {
                throw new IllegalStateException("Required configuration key '" + requiredKey + "' may not be null.");
            }
        }
    }

    private static void loadServerSettings(final Configuration.Builder builder, final Map<String, Object> stormConfig) {
        builder
            .withHost((String) stormConfig.get(REDIS_SERVER_HOST))
            .withPort(stormConfig.get(REDIS_SERVER_PORT));

        if (stormConfig.containsKey(REDIS_SERVER_PASSWORD)) {
            final String password = (String) stormConfig.get(REDIS_SERVER_PASSWORD);
            if (password != null && !password.trim().isEmpty()) {
                builder.withPassword(password);
            }
        }
    }

    private static void loadConsumerSettings(
        final Configuration.Builder builder, final Map<String, Object> stormConfig, final TopologyContext topologyContext) {
        builder
            .withGroupName((String) stormConfig.get(REDIS_CONSUMER_GROUP_NAME))
            .withStreamKey((String) stormConfig.get(REDIS_CONSUMER_STREAM_KEY));

        // Grab consumerId prefix from config.
        final String consumerIdPrefix = (String) stormConfig.get(REDIS_CONSUMER_CONSUMER_ID_PREFIX);

        // Determine how many instances we have via topology context.
        final int instanceId = topologyContext.getThisTaskIndex();

        // Define unique consumerId using prefix + instanceId
        builder.withConsumerId(consumerIdPrefix + instanceId);
    }

    private static void loadOptionalSettings(final Configuration.Builder builder, final Map<String, Object> stormConfig) {
        // How long to delay between cycles in the consumer.
        if (stormConfig.containsKey(CONSUMER_DELAY_MILLIS)) {
            final Number delayMillis = (Number) stormConfig.get(CONSUMER_DELAY_MILLIS);
            builder.withConsumerDelayMillis(delayMillis.longValue());
        }

        // Size of internal bounded queue for processing acks.
        if (stormConfig.containsKey(CONSUMER_MAX_ACK_QUEUE_SIZE)) {
            final Number maxSize = (Number) stormConfig.get(CONSUMER_MAX_ACK_QUEUE_SIZE);
            builder.withMaxAckQueueSize(maxSize.intValue());
        }

        // Size of internal bounded queue for un-emitted tuples.
        if (stormConfig.containsKey(CONSUMER_MAX_TUPLE_QUEUE_SIZE)) {
            final Number maxSize = (Number) stormConfig.get(CONSUMER_MAX_TUPLE_QUEUE_SIZE);
            builder.withMaxTupleQueueSize(maxSize.intValue());
        }
    }
}
