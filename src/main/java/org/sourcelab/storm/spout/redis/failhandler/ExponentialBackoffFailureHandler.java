package org.sourcelab.storm.spout.redis.failhandler;

import org.sourcelab.storm.spout.redis.FailureHandler;
import org.sourcelab.storm.spout.redis.Message;

import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.TreeMap;

/**
 * Will re-attempt playing previously failed messages after an exponential back off period.
 */
public class ExponentialBackoffFailureHandler implements FailureHandler {
    /**
     * Configuration properties.
     */
    private final ExponentialBackoffConfig config;

    /**
     * Used to control timing around retries.
     * Also allows for injecting a mock clock for testing.
     */
    private transient Clock clock = Clock.systemUTC();

    /**
     * This map hows how many times each messageId has failed.
     */
    private transient Map<String, Integer> numberOfTimesFailed;

    /**
     * This is a sorted Tree of timestamps, where each timestamp points to a queue of
     * failed messages.
     */
    private transient TreeMap<Long, Queue<Message>> failedMessages;

    /**
     * Constructor.
     * @param config Defines configuration properties.
     */
    public ExponentialBackoffFailureHandler(final ExponentialBackoffConfig config) {
        this.config = Objects.requireNonNull(config);
    }

    /**
     * Constructor.
     * @param config Defines configuration properties.
     */
    public ExponentialBackoffFailureHandler(final ExponentialBackoffConfig.Builder config) {
        this(Objects.requireNonNull(config).build());
    }

    @Override
    public void open(final Map<String, Object> stormConfig) {
        numberOfTimesFailed = new HashMap<>();
        failedMessages = new TreeMap<>();
        clock = Clock.systemUTC();
    }

    @Override
    public boolean fail(final Message message) {
        // Validate input.
        if (message == null) {
            return false;
        }

        // If we shouldn't retry this message again
        final boolean result = isEligibleForRetry(message.getId());
        if (!result) {
            // Return a value of false. Spout will call ack() on the message shortly.
            return false;
        }

        // Increment our failure counter for this message.
        final String messageId = message.getId();
        final int failCount = numberOfTimesFailed.compute(messageId, (key, val) -> (val == null) ? 1 : val + 1);

        // Determine the timestamp that this message should be replayed.
        long additionalTime = (long) (config.getInitialRetryDelayMs() * Math.pow(config.getRetryDelayMultiplier(), failCount - 1));

        // If its over our configured max delay, cap at the configured max delay period.
        additionalTime = Long.min(additionalTime, config.getRetryDelayMaxMs());

        // Calculate the timestamp for the retry.
        final long retryTimestamp = clock.millis() + additionalTime;

        // Clear queues of previous fails.
        if (failCount > 1) {
            for (final Queue queue : failedMessages.values()) {
                if (queue.remove(message)) {
                    break;
                }
            }
        }

        // Get / Create the queue for this timestamp and append our message.
        final Queue<Message> queue = failedMessages.computeIfAbsent(retryTimestamp, k -> new LinkedList<>());
        queue.add(message);

         // Return value of true
        return true;
    }

    @Override
    public void ack(final String messageId) {
        // Stop tracking how many times this messageId has failed.
        numberOfTimesFailed.remove(messageId);
    }

    @Override
    public Message getMessage() {
        // If we have no queues
        if (failedMessages.isEmpty()) {
            // Then nothing to return
            return null;
        }

        // Grab current timestamp
        final long now = clock.millis();

        // Grab the lowest key from the sorted map.
        final Map.Entry<Long, Queue<Message>> entry = failedMessages.firstEntry();
        if (entry == null) {
            return null;
        }

        final long lowestTimestampKey = entry.getKey();
        final Queue<Message> queue = entry.getValue();

        // Determine if the key (timestamp) has expired or not
        // Here we define 'expired' as having a timestamp less than or equal to now.
        if (lowestTimestampKey > now) {
            // Nothing has expired.
            return null;
        }

        // Grab next message from the queue.
        final Message message = queue.poll();

        // If our queue is empty
        if (queue.isEmpty()) {
            // Clean up.
            failedMessages.remove(lowestTimestampKey);
        }

        // Return the message
        return message;
    }

    /**
     * Determine if this messageId is eleigible for being retried or not.
     * @param messageId The messageId to check.
     * @return True if the messageId should be retried later, false if it should not.
     */
    private boolean isEligibleForRetry(final String messageId) {
        // Never retry if configured with a value of 0.
        if (config.getRetryLimit() == 0) {
            return false;
        }

        // Always retry if configured with a value less than 0.
        if (config.getRetryLimit() < 0) {
            return true;
        }

        // Determine if this message has failed previously.
        final int numberOfTimesHasFailed = numberOfTimesFailed.getOrDefault(messageId, 0);

        // If we have exceeded the retry limit
        if (numberOfTimesHasFailed >= config.getRetryLimit()) {
            // Don't retry
            return false;
        }
        return true;
    }

    /**
     * For injecting a clock implementation during tests.
     * @param clock the clock implementation to use.
     */
    void setClock(final Clock clock) {
        this.clock = Objects.requireNonNull(clock);
    }

    /**
     * Protected accessor for validation within tests.
     */
    TreeMap<Long, Queue<Message>> getFailedMessages() {
        return failedMessages;
    }

    /**
     * Protected accessor for validation within tests.
     */
    Map<String, Integer> getNumberOfTimesFailed() {
        return Collections.unmodifiableMap(numberOfTimesFailed);
    }
}
