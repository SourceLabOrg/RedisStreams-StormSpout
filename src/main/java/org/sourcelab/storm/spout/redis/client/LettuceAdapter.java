package org.sourcelab.storm.spout.redis.client;

import io.lettuce.core.api.sync.RedisStreamCommands;

/**
 * Adapter to allow usage of both RedisClient and RedisClusterClient.
 */
public interface LettuceAdapter {

    /**
     * Is the underlying client connected?
     * @return true if connected, false if not.
     */
    boolean isConnected();

    /**
     * Call connect.
     */
    void connect();

    /**
     * Get sync Redis Stream Commands instance.
     */
    RedisStreamCommands<String, String> getSyncCommands();

    /**
     * Call shutdown.
     */
    void shutdown();
}
