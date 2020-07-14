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
    private Map<String, Integer> numberOfTimesFailed;

    /**
     * This is a sorted Tree of timestamps, where each timestamp points to a queue of
     * failed messages.
     */
    private TreeMap<Long, Queue<Message>> failedMessages;

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
        // Init data structures.
        numberOfTimesFailed = new HashMap<>();
        failedMessages = new TreeMap<>();

        if (clock == null) {
            clock = Clock.systemUTC();
        }
    }

    @Override
    public boolean fail(final Message message) {
        // Validate input.
        if (message == null) {
            return false;
        }

        // Determine if we should continue to track/handle this message.
        final boolean result = retryFurther(message.getId());
        if (!result) {
            // If not, return false.  Spout will call ack() on the message shortly.
            return false;
        }

        // Determine how many times this message has failed previously.
        final String messageId = message.getId();
        final int failCount = numberOfTimesFailed.compute(messageId, (key, val) -> (val == null) ? 1 : val + 1);

        // Determine when we should retry this msg next
        // Calculate how many milliseconds to wait until the next retry
        long additionalTime = (long) (config.getInitialRetryDelayMs() * Math.pow(config.getRetryDelayMultiplier(), failCount - 1));

        // If its over our configured max delay, cap at the configured max delay period.
        additionalTime = Long.min(additionalTime, config.getRetryDelayMaxMs());

        // Calculate the timestamp for the retry.
        final long retryTime = clock.millis() + additionalTime;

        // If we had previous fails
        if (failCount > 1) {
            // Make sure they're removed.  This kind of sucks.
            // This may not be needed in reality...just because of how we've setup our tests :/
            for (final Queue queue : failedMessages.values()) {
                if (queue.remove(message)) {
                    break;
                }
            }
        }

        // Grab the queue for this timestamp,
        // If it doesn't exist, create a new queue and return it.
        final Queue<Message> queue = failedMessages.computeIfAbsent(retryTime, k -> new LinkedList<>());

        // Add our message to the queue.
        queue.add(message);

         // Return value of true
        return true;
    }

    @Override
    public void ack(final String messageId) {
        // Remove fail count tracking
        numberOfTimesFailed.remove(messageId);
    }

    @Override
    public Message getMessage() {
        // If our map is empty
        if (failedMessages.isEmpty()) {
            // Then we have nothing to expire!
            return null;
        }

        // Grab current timestamp
        final long now = clock.millis();

        // Grab the lowest key from the sorted map.
        // Because of the empty check above, we're confident this will NOT return null.
        final Map.Entry<Long, Queue<Message>> entry = failedMessages.firstEntry();

        // But lets be safe.
        if (entry == null) {
            // Nothing to expire
            return null;
        }

        // Populate the values
        final long lowestTimestampKey = entry.getKey();
        final Queue<Message> queue = entry.getValue();

        // Determine if the key (timestamp) has expired or not
        // Here we define 'expired' as having a timestamp less than or equal to now.
        if (lowestTimestampKey > now) {
            // Nothing has expired.
            return null;
        }

        // Pop a message from the queue.
        final Message message = queue.poll();

        // If our queue is now empty
        if (queue.isEmpty()) {
            // remove it
            failedMessages.remove(lowestTimestampKey);
        }

        // Return the message
        return message;
    }

    private boolean retryFurther(final String messageId) {
        // If max retries is set to 0, we will never retry any tuple.
        if (config.getRetryLimit() == 0) {
            return false;
        }

        // If max retries is less than 0, we'll retry forever
        if (config.getRetryLimit() < 0) {
            return true;
        }

        // Find out how many times this tuple has failed previously.
        final int numberOfTimesHasFailed = numberOfTimesFailed.getOrDefault(messageId, 0);

        // If we have exceeded our max retry limit
        if (numberOfTimesHasFailed >= config.getRetryLimit()) {
            // Then we shouldn't retry
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
