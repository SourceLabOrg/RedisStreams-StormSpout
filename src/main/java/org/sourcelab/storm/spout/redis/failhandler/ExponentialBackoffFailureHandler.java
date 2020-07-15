package org.sourcelab.storm.spout.redis.failhandler;

import com.codahale.metrics.Counter;
import org.apache.storm.task.TopologyContext;
import org.sourcelab.storm.spout.redis.FailureHandler;
import org.sourcelab.storm.spout.redis.Message;

import java.time.Clock;
import java.util.Collection;
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
    private transient Clock clock;

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
     * Handles recording metrics.
     */
    private transient MetricHandler metricHandler;

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
    public void open(final Map<String, Object> stormConfig, final TopologyContext topologyContext) {
        numberOfTimesFailed = new HashMap<>();
        failedMessages = new TreeMap<>();
        if (clock == null) {
            clock = Clock.systemUTC();
        }

        // Initialize metrics.
        if (config.isMetricsEnabled()) {
            metricHandler = new MetricHandler(topologyContext, failedMessages);
        } else {
            // Setup no-op metric collection.
            metricHandler = new MetricHandler();
        }
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
            // But lets remove from our tracking set now, this allows us to differentiate between
            // successfully retries in the ack() method vs exceeded retry limits.
            numberOfTimesFailed.remove(message.getId());
            metricHandler.incrExceededRetryLimitCounter();
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
        // If we were tracking this messageId
        if (numberOfTimesFailed.remove(messageId) != null) {
            // It means it was a successful retry.
            metricHandler.incrSuccessfulRetriedMessagesCounter();
        }
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

        // If our queue is empty after popping this message
        if (queue.isEmpty()) {
            // Lets cleanup and remove the queue.
            failedMessages.remove(lowestTimestampKey);
        }

        // Return the message, which may be null
        if (message != null) {
            // If not null, that means we're replying a previously failed message.
            metricHandler.incrRetriedMessagesCounter();
        }
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

    /**
     * Helper class to handle metrics.
     */
    private static class MetricHandler {
        private final Counter metricExceededRetryLimitCount;
        private final Counter metricRetriedMessagesCount;
        private final Counter metricSuccessfulRetriedMessagesCounter;

        /**
         * Constructor to setup a No-op.
         * Used for when metrics are disabled.
         */
        public MetricHandler() {
            metricExceededRetryLimitCount = null;
            metricRetriedMessagesCount = null;
            metricSuccessfulRetriedMessagesCounter = null;
        }

        /**
         * Constructor to initialize metric collection.
         * @param topologyContext to register metrics.
         * @param retryQueue Our internal retry queue to collect size data.
         */
        public MetricHandler(final TopologyContext topologyContext, final TreeMap<Long, Queue<Message>> retryQueue) {
            metricExceededRetryLimitCount = topologyContext.registerCounter("failureHandler_exceededRetryLimit");
            metricRetriedMessagesCount = topologyContext.registerCounter("failureHandler_retriedMessages");
            metricSuccessfulRetriedMessagesCounter = topologyContext.registerCounter("failureHandler_successfulRetriedMessages");
            topologyContext.registerGauge("failureHandler_queuedForRetry", () ->
                retryQueue.values().stream()
                .mapToLong(Collection::size)
                .sum()
            );
        }

        public void incrExceededRetryLimitCounter() {
            if (metricExceededRetryLimitCount == null) {
                return;
            }
            metricExceededRetryLimitCount.inc();
        }

        public void incrRetriedMessagesCounter() {
            if (metricRetriedMessagesCount == null) {
                return;
            }
            metricRetriedMessagesCount.inc();
        }

        public void incrSuccessfulRetriedMessagesCounter() {
            if (metricSuccessfulRetriedMessagesCounter == null) {
                return;
            }
            metricSuccessfulRetriedMessagesCounter.inc();
        }
    }
}
