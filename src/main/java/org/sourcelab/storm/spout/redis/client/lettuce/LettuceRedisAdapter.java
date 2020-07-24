package org.sourcelab.storm.spout.redis.client.lettuce;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisStreamCommands;

import java.util.Objects;

/**
 * Adapter for talking to a single Redis instance.
 * If you need to talk to a RedisCluster {@link LettuceClusterAdapter}.
 */
public class LettuceRedisAdapter implements LettuceAdapter {

    /**
     * The underlying Redis Client.
     */
    private final RedisClient redisClient;

    /**
     * Underlying connection objects.
     */
    private StatefulRedisConnection<String, String> connection;
    private RedisStreamCommands<String, String> syncCommands;

    public LettuceRedisAdapter(final RedisClient redisClient) {
        this.redisClient = Objects.requireNonNull(redisClient);
    }

    @Override
    public boolean isConnected() {
        return connection != null;
    }

    @Override
    public void connect() {
        if (isConnected()) {
            throw new IllegalStateException("Cannot call connect more than once!");
        }
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
            syncCommands = null;
            connection.close();
            connection = null;
        }
        redisClient.shutdown();
    }
}
