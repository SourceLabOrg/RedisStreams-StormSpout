package org.sourcelab.storm.spout.redis.funnel;

/**
 * Funnel from the Spout's point of view.
 *
 * Used by the Spout to:
 *   - Retrieve the next message that should be emitted by the spout.
 *   - Push an Acknowledgement of a message being processed.
 *   - Push a notification that a message has failed.
 *   - Request the background thread to stop.
 */
public interface SpoutFunnel {
    /**
     * Ask for the next message which should be emitted by the spout.
     * @return Message to be emitted.  A value of NULL means no message is ready to be emitted.
     */
    Message nextMessage();

    /**
     * Called to acknowledge a message as having been processed successfully.
     * @param msgId Id of the message.
     * @return true if accepted.
     */
    boolean ackMessage(final String msgId);

    /**
     * Called to notify that a message has failed to process.
     * @param msgId Id of the message.
     * @return true if accepted.
     */
    boolean failMessage(final String msgId);

    /**
     * Called to request the background consuming thread to shutdown.
     */
    void requestStop();
}
