package org.sourcelab.storm.spout.redis.client.jedis;

import redis.clients.jedis.StreamEntry;

import java.util.List;
import java.util.Map;

/**
 * Adapter to allow usage of both Jedis and JedisCluster.
 */
public interface JedisAdapter {
    /**
     * Call connect.
     */
    void connect();

    /**
     * Consume next batch of messages.
     * @return List of messages consumed.
     */
    List<Map.Entry<String, List<StreamEntry>>> consume();

    /**
     * Mark the provided messageId as acknowledged/completed.
     * @param msgId Id of the message.
     */
    void commit(final String msgId);

    /**
     * Disconnect client.
     */
    void close();

    /**
     * Advance the last offset consumed from PPL.
     * @param lastMsgId Id of the last msg consumed.
     */
    void advancePplOffset(final String lastMsgId);

    /**
     * Switch to consuming from latest messages.
     */
    void switchToConsumerGroupMessages();
}
