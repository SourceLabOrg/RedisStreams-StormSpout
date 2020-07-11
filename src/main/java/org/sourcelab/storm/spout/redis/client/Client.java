package org.sourcelab.storm.spout.redis.client;

import org.sourcelab.storm.spout.redis.Message;

import java.util.List;

/**
 * Abstraction on top of underlying RedisClient library.
 */
public interface Client {
    /**
     * Connect to redis server.
     */
    void connect();

    /**
     * Retrieve the next batch of messages from Redis Stream.
     */
    List<Message> nextMessages();

    /**
     * Mark message with passed Id as having been processed.
     * @param msgId Id of the message to mark complete.
     */
    void commitMessage(final String msgId);

    /**
     * Disconnect from Redis server.
     */
    void disconnect();
}
