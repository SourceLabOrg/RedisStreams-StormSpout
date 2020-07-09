package org.sourcelab.storm.spout.redis.util;

import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sourcelab.storm.spout.redis.ClientConfiguration;

import java.util.Map;
import java.util.Objects;

/**
 * Build ClientConfiguration from Storm config maps.
 */
public class StormToClientConfigurationUtil {
    private static final Logger logger = LoggerFactory.getLogger(StormToClientConfigurationUtil.class);

    // Connection Details
    public static String REDIS_SERVER_HOST = "redis_stream_spout.server.host";
    public static String REDIS_SERVER_PORT = "redis_stream_spout.server.port";
    public static String REDIS_SERVER_PASSWORD = "redis_stream_spout.server.password";

    // Consumer Details
    public static String REDIS_CONSUMER_GROUP_NAME = "redis_stream_spout.consumer.group_name";
    public static String REDIS_CONSUMER_CONSUMER_ID_PREFIX = "redis_stream_spout.consumer.consumer_id_prefix";
    public static String REDIS_CONSUMER_STREAM_KEY = "redis_stream_spout.consumer.stream_key";

    // Tuple Conversion
    public static String REDIS_TUPLE_CONVERTER_CLASS = "redis_stream_spout.tuple_converter_class";

    // Other settings
    // TODO load remaining settings.

    public static ClientConfiguration load(final Map<String, Object> stormConfig, final TopologyContext topologyContext) {
        Objects.requireNonNull(stormConfig);
        Objects.requireNonNull(topologyContext);

        // Validate required keys exist.
        validateRequiredKeys(stormConfig);

        final ClientConfiguration.Builder builder = ClientConfiguration.newBuilder();
        loadTupleConverter(builder, stormConfig);
        loadServerSettings(builder, stormConfig);
        loadConsumerSettings(builder, stormConfig, topologyContext);

        return builder.build();
    }

    private static void loadTupleConverter(final ClientConfiguration.Builder builder, final Map<String, Object> stormConfig) {
        String classStr = (String) stormConfig.get(REDIS_TUPLE_CONVERTER_CLASS);
        if (classStr == null || classStr.trim().isEmpty()) {
            throw new IllegalStateException("Invalid value for key '" + REDIS_TUPLE_CONVERTER_CLASS + "'");
        }
        builder.withTupleConverterClass(classStr);
    }

    private static void validateRequiredKeys(final Map<String, Object> stormConfig) {
        final String[] requiredKeys = new String[] {
            // Server keys
            REDIS_SERVER_HOST, REDIS_SERVER_PORT,

            // Consumer keys
            REDIS_CONSUMER_GROUP_NAME, REDIS_CONSUMER_STREAM_KEY,

            // Other
            REDIS_TUPLE_CONVERTER_CLASS
        };

        for (final String requiredKey : requiredKeys) {
            if (!stormConfig.containsKey(requiredKey)) {
                throw new IllegalStateException("Missing required configuration key '" + requiredKey + "'");
            }
        }
    }

    private static void loadServerSettings(final ClientConfiguration.Builder builder, final Map<String, Object> stormConfig) {
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

    private static void loadConsumerSettings(final ClientConfiguration.Builder builder, final Map<String, Object> stormConfig, final TopologyContext topologyContext) {
        builder
            .withGroupName((String) stormConfig.get(REDIS_CONSUMER_GROUP_NAME))
            .withStreamKey((String) stormConfig.get(REDIS_CONSUMER_STREAM_KEY));

        String consumerIdPrefix = null;
        if (stormConfig.containsKey(REDIS_CONSUMER_CONSUMER_ID_PREFIX)) {
            consumerIdPrefix = (String) stormConfig.get(REDIS_CONSUMER_CONSUMER_ID_PREFIX);
        }
        if (consumerIdPrefix == null || consumerIdPrefix.trim().isEmpty()) {
            consumerIdPrefix = "storm_consumer";
        }

        // Determine how many instances we have via topology context.
        final int instanceId = topologyContext.getThisTaskIndex();

        // Define unique consumerId using prefix + instanceId
        builder.withConsumerId(consumerIdPrefix + instanceId);
    }
}