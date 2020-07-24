package org.sourcelab.storm.spout.redis.client.lettuce;

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
     * @return Available synchronous stream commands.
     */
    RedisStreamCommands<String, String> getSyncCommands();

    /**
     * Call shutdown.
     */
    void shutdown();
}
