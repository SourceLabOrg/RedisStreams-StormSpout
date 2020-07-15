package org.sourcelab.storm.spout.redis.client;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisStreamCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Adapter for RedisClient.
 */
public class LettuceRedisClient implements LettuceAdapter {
    private static final Logger logger = LoggerFactory.getLogger(LettuceRedisClient.class);

    /**
     * The underlying Redis Client.
     */
    private final RedisClient redisClient;

    /**
     * Underlying connection objects.
     */
    private StatefulRedisConnection<String, String> connection;
    private RedisStreamCommands<String, String> syncCommands;

    public LettuceRedisClient(final RedisClient redisClient) {
        this.redisClient = Objects.requireNonNull(redisClient);
    }

    @Override
    public boolean isConnected() {
        return connection != null;
    }

    @Override
    public void connect() {
        connection = redisClient.connect();
    }

    @Override
    public RedisStreamCommands<String, String> getSyncCommands() {
        if (syncCommands == null) {
            syncCommands = connection.sync();
        }
        return syncCommands;
    }

    @Override
    public void shutdown() {
        // Close our connection and shutdown.
        if (connection != null) {
            connection.close();
            connection = null;
        }
        redisClient.shutdown();
    }
}
