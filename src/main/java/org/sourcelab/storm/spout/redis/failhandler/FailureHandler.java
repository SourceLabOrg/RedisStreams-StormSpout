package org.sourcelab.storm.spout.redis.failhandler;

import org.sourcelab.storm.spout.redis.funnel.Message;

import java.util.Map;

/**
 * For handling failed tuples.
 * Does NOT need to be thread safe as only accessed via a single thread.
 */
public interface FailureHandler {
    void open(Map<String, Object> stormConfig);

    /**
     * Handle a failed message.
     * If the implementation wants to replay this tuple again in the future, it should return value of TRUE.
     * If the implementation does NOT want to handle/replay this tuple in the future, it should return a value of FALSE.
     * @param message The failed message.
     * @return True if the implementation will replay this tuple again later, false if not.
     */
    boolean fail(final Message message);

    /**
     * Called for MessageId's that have successfully finished processing.
     * @param msgId id corresponding to the Message that has finished processing.
     */
    void ack(final String msgId);

    /**
     * Return a previously failed message to be replayed. or NULL if no such messages are ready to be replayed.
     * @return Previously failed message or NULL.
     */
    Message getMessage();
}
