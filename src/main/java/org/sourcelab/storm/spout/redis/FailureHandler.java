package org.sourcelab.storm.spout.redis;

import java.io.Serializable;
import java.util.Map;

/**
 * For handling failed tuples.
 * Does NOT need to be thread safe as only accessed via a single thread.
 */
public interface FailureHandler extends Serializable {
    /**
     * Lifecycle method.  Called once the Spout has started.
     * @param stormConfig Configuration map passed from the spout.
     */
    void open(final Map<String, Object> stormConfig);

    /**
     * Handle a failed message.
     * A return value of TRUE means:
     *   This implementation wants to replay this message again in the future.
     *   The spout will NOT mark the message as processed/ack the message.
     *
     * A return value of FALSE means:
     *   This implementation does NOT want to replay this message in the future.
     *   The spout WILL mark the message as processed/ack the message.
     *
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
