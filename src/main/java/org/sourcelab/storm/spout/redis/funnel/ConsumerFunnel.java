package org.sourcelab.storm.spout.redis.funnel;

/**
 * Funnel from the point of the Redis Consumer.
 *
 * Used by the Client to:
 *  - Push Messages down to the Spout instance.
 *  - Retrieve messageIds of messages that have been processed by the topology.
 *  - Determine if the background thread should shutdown.
 */
public interface ConsumerFunnel {
    /**
     * Pushes a message down to the Spout thread to be emitted.
     * @param message The message to emit.
     * @return true if accepted.
     */
    boolean addMessage(final Message message);

    /**
     * Get the next messageId that should be recorded as processed.
     * @return MessageId of message to record as having been processed.
     */
    String getNextAck();

    /**
     * Used to determine if the background consuming thread should stop processing and shut down.
     *
     * @return True if the background process should terminate, false if it should continue.
     */
    boolean shouldStop();

    /**
     * Used to set the state of the background processing thread.
     *
     * @param state Set to true to mark the background process as actively running, false to mark as no longer running.
     */
    void setIsRunning(boolean state);
}
