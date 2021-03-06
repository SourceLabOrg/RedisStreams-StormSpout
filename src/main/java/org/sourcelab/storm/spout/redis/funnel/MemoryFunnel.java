package org.sourcelab.storm.spout.redis.funnel;

import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sourcelab.storm.spout.redis.FailureHandler;
import org.sourcelab.storm.spout.redis.Message;
import org.sourcelab.storm.spout.redis.RedisStreamSpoutConfig;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Funnels tuples and acks between the Spout thread and the Consumer thread in a
 * thread safe manner.
 */
public class MemoryFunnel implements SpoutFunnel, ConsumerFunnel {
    private static final Logger logger = LoggerFactory.getLogger(MemoryFunnel.class);

    /**
     * Tracks Tuples in Flight.
     * Needs to be concurrent because of metrics collection.
     */
    private final Map<String, Message> inFlightTuples;

    /**
     * BlockingQueue for tuples.
     * Needs to be concurrent because it is modified by both threads.
     */
    private final LinkedBlockingQueue<Message> tupleQueue;

    /**
     * BlockingQueue for acks.
     * Needs to be concurrent because it is modified by both threads.
     */
    private final LinkedBlockingQueue<String> ackQueue;

    /**
     * How to handle failures.
     */
    private final FailureHandler failureHandler;

    /**
     * Stop flags.
     */
    private final AtomicBoolean shouldStop = new AtomicBoolean(false);
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    /**
     * Constructor.
     * @param config Configuration properties.
     * @param stormConfig Configuration map passed from the spout.
     * @param topologyContext Storm TopologyContext instance.
     */
    public MemoryFunnel(
        final RedisStreamSpoutConfig config,
        final Map<String, Object> stormConfig,
        final TopologyContext topologyContext
    ) {
        Objects.requireNonNull(config);
        Objects.requireNonNull(config);
        Objects.requireNonNull(topologyContext);

        // These DO need to be concurrent.
        inFlightTuples = new ConcurrentHashMap<>(config.getMaxTupleQueueSize());
        tupleQueue = new LinkedBlockingQueue<>(config.getMaxTupleQueueSize());
        ackQueue = new LinkedBlockingQueue<>(config.getMaxAckQueueSize());

        // Create failure handler instance
        failureHandler = config.getFailureHandler();
        failureHandler.open(stormConfig, topologyContext);

        // Initialize Metrics
        if (config.isMetricsEnabled()) {
            topologyContext.registerGauge("tupleQueueSize", tupleQueue::size);
            topologyContext.registerGauge("ackQueueSize", ackQueue::size);
            topologyContext.registerGauge("inFlightTuples", inFlightTuples::size);
        }
    }

    @Override
    public Message nextMessage() {
        // Should replay a failed tuple?
        Message nextMessage = failureHandler.getMessage();

        // If the failureHandler has nothing to emit
        if (nextMessage == null) {
            // Pop off of tuple queue
            nextMessage = tupleQueue.poll();
        }

        // If nothing pop'd from the queue
        // then the queue is empty.
        if (nextMessage == null) {
            return null;
        }

        // Add to inflight tuples map
        inFlightTuples.put(nextMessage.getId(), nextMessage);

        // return message
        return nextMessage;
    }

    @Override
    public boolean ackMessage(final String msgId) {
        if (msgId == null) {
            return false;
        }

        // Add to acked tuples queue,
        // If the queue is full, this will block.
        try {
            // Notify the failure handler that this msgId was acked.
            failureHandler.ack(msgId);

            // Add to the ackQueue
            ackQueue.put(msgId);
        } catch (final InterruptedException exception) {
            logger.error("Interrupted while attempting to add to Ack Queue: {}", exception.getMessage(), exception);
        }

        // remove from inflight tuples map.
        inFlightTuples.remove(msgId);

        return true;
    }

    @Override
    public boolean failMessage(final String msgId) {
        if (msgId == null) {
            return false;
        }

        // remove from inflight tuples map.
        final Message failedTuple = inFlightTuples.remove(msgId);

        // Unable to find a tuple with that msgId
        if (failedTuple == null) {
            return false;
        }

        // Add to failed tuples thing
        final boolean result = failureHandler.fail(failedTuple);

        // If the result is false, we should ack the message
        if (result == false) {
            ackMessage(msgId);
        }

        // And return the result
        return result;
    }

    @Override
    public void requestStop() {
        shouldStop.set(true);

        // Wait until stopped.
        final long limit = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);
        while (isRunning.get()) {
            try {
                Thread.sleep(250L);
                if (System.currentTimeMillis() >= limit) {
                    throw new RuntimeException("Timed out waiting for thread to complete.");
                }
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    /**
     * Add a message to the queue.
     * By design this will block once the buffer becomes full to apply backpressure to
     * the consumer thread.
     */
    @Override
    public boolean addMessage(final Message message) {
        // This may block.
        try {
            tupleQueue.put(message);
            return true;
        } catch (final InterruptedException exception) {
            logger.error("Interrupted while attempting to add to Message Queue: {}", exception.getMessage(), exception);
        }
        return false;
    }

    /**
     * Get the next MessageId that has been marked as successfully completed.
     * @return Id of the message, or NULL if buffer is empty.
     */
    @Override
    public String nextAck() {
        return ackQueue.poll();
    }

    @Override
    public boolean shouldStop() {
        return shouldStop.get();
    }

    @Override
    public void setIsRunning(boolean state) {
        isRunning.set(state);
    }

    /**
     * Accessor for running state.
     * @return true if the client thread is processing, false if not.
     */
    public boolean isRunning() {
        return isRunning.get();
    }
}
