package org.sourcelab.storm.spout.redis.failhandler;

import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sourcelab.storm.spout.redis.FailureHandler;
import org.sourcelab.storm.spout.redis.Message;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Handler which will replay failed tuples a maximum number of times.
 *
 * A value greater than 0 sets the upper limit on the number of times a message will fail before just being skipped.
 * A value equal to 0 says failed messages will NEVER be replayed.
 * A value less than 0 says ALWAYS replay failed messages until they are successful.
 */
public class RetryFailedTuples implements FailureHandler, Serializable {
    private static final Logger logger = LoggerFactory.getLogger(RetryFailedTuples.class);

    /**
     * This tracks how many times a specific msgId has been replayed.
     * MsgId => Number of times we've replayed it.
     */
    private final Map<String, Long> messageCounter = new HashMap<>();

    /**
     * How many times a failed message should be replayed.
     * A value of 0 means never give up on a message and always replay it.
     */
    private final int maxRetries;

    /**
     * Contains a FIFO queue for failed messages.
     */
    private transient LinkedBlockingQueue<Message> messageQueue;

    /**
     * Constructor.
     * @param maxRetries Maximum number of times to retry a failed tuple.
     */
    public RetryFailedTuples(final int maxRetries) {
        this.maxRetries = maxRetries;
    }

    @Override
    public void open(final Map<String, Object> stormConfig, final TopologyContext topologyContext) {
        // Create queue
        messageQueue = new LinkedBlockingQueue<>();
    }

    @Override
    public boolean fail(final Message message) {
        if (message == null) {
            return false;
        }

        // Get it's Id.
        final String msgId = message.getId();

        // Determine if we should replay it.
        if (!shouldReplay(msgId)) {
            // return false
            return false;
        }

        // We do want to replay it, so drop into our counter and queue.
        try {
            // Only track if we have a limited number.
            if (maxRetries > 0) {
                messageCounter.compute(msgId, (key, value) -> (value == null) ? 1L : value + 1L);
            }
            messageQueue.put(message);
        } catch (final InterruptedException exception) {
            logger.error("Interrupted while attempting to add to Failure Queue: {}", exception.getMessage(), exception);
            messageCounter.remove(msgId);
            return false;
        }

        // return true.
        return true;
    }

    private boolean shouldReplay(final String msgId) {
        // If max retries is 0, we should never replay.
        if (maxRetries == 0) {
            return false;
        }

        // If max retries is less than 0
        if (maxRetries < 0) {
            // We should always replay
            return true;
        }

        final long previousFailures = messageCounter.getOrDefault(msgId, 0L);
        if (previousFailures >= maxRetries) {
            return false;
        }
        return true;
    }

    @Override
    public void ack(final String msgId) {
        // Remove from our tracking map
        messageCounter.remove(msgId);
    }

    @Override
    public Message getMessage() {
        return messageQueue.poll();
    }
}
