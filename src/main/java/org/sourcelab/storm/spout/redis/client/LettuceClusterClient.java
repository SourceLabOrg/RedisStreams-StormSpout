package org.sourcelab.storm.spout.redis.client;

import io.lettuce.core.api.sync.RedisStreamCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;

import java.util.Objects;

/**
 *
 */
public class LettuceClusterClient implements LettuceAdapter {
    /**
     * The underlying Redis Client.
     */
    private final RedisClusterClient redisClient;

    /**
     * Underlying connection objects.
     */
    private StatefulRedisClusterConnection<String, String> connection;
    private RedisStreamCommands<String, String> syncCommands;

    public LettuceClusterClient(final RedisClusterClient redisClient) {
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
