package org.sourcelab.storm.spout.redis.client;

import io.lettuce.core.api.sync.RedisStreamCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;

import java.util.Objects;

/**
 * Adapter for talking to a RedisCluster.
 * If you need to talk to a single Redis instance {@link LettuceRedisAdapter}.
 */
public class LettuceClusterAdapter implements LettuceAdapter {
    /**
     * The underlying Redis Client.
     */
    private final RedisClusterClient redisClient;

    /**
     * Underlying connection objects.
     */
    private StatefulRedisClusterConnection<String, String> connection;
    private RedisStreamCommands<String, String> syncCommands;

    public LettuceClusterAdapter(final RedisClusterClient redisClient) {
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
