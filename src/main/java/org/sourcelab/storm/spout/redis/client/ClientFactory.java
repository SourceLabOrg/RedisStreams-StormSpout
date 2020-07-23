package org.sourcelab.storm.spout.redis.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sourcelab.storm.spout.redis.RedisStreamSpoutConfig;
import org.sourcelab.storm.spout.redis.client.jedis.JedisClient;
import org.sourcelab.storm.spout.redis.client.lettuce.LettuceClient;

import java.util.Objects;

/**
 * Factory for creating the appropriate Client instance based on config.
 */
public class ClientFactory {
    private static final Logger logger = LoggerFactory.getLogger(ClientFactory.class);

    /**
     * Create the appropriate client intance based on configuration.
     * @param config Spout configuration.
     * @param instanceId Instance id of spout.
     * @return Client.
     */
    public Client createClient(final RedisStreamSpoutConfig config, final int instanceId) {
        Objects.requireNonNull(config);

        switch (config.getClientType()) {
            case JEDIS:
                logger.info("Using Jedis client library.");
                return new JedisClient(config, instanceId);
            case LETTUCE:
                logger.info("Using Lettuce client library.");
                return new LettuceClient(config, instanceId);
            default:
                throw new IllegalStateException("Unknown/Unhandled Client Type");
        }
    }
}
