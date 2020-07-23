package org.sourcelab.storm.spout.redis.client.jedis;

import redis.clients.jedis.StreamEntry;

import java.util.List;
import java.util.Map;

/**
 * Adapter to allow usage of both Jedis and JedisCluster.
 */
public interface JedisAdapter {
    /**
     * Is the underlying client connected?
     * @return true if connected, false if not.
     */
    boolean isConnected();

    /**
     * Call connect.
     */
    void connect();

    List<Map.Entry<String, List<StreamEntry>>> consume();

    void commit(final String msgId);

    /**
     * Call shutdown.
     */
    void close();

    void switchToPpl(final String lastMsgId);

    void switchToConsumerGroupMessages();
}
