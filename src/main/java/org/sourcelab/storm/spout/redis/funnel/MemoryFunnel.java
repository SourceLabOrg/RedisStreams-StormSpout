package org.sourcelab.storm.spout.redis.funnel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sourcelab.storm.spout.redis.ClientConfiguration;
import org.sourcelab.storm.spout.redis.failhandler.FailureHandler;
import org.sourcelab.storm.spout.redis.failhandler.NoRetryHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Funnels tuples and acks between the Spout thread and the Consumer thread.
 */
public class MemoryFunnel implements SpoutFunnel, ConsumerFunnel {
    private static final Logger logger = LoggerFactory.getLogger(MemoryFunnel.class);

    /**
     * Tracks Tuples in Flight.
     * Does NOT need to be concurrent because only modified via Spout side.
     */
    private final Map<String, Message> inFlightTuples = new HashMap<>();

    /**
     * BlockingQueue for tuples.
     * Needs to be concurrent because it is modified by both threads.
     */
    private final Queue<Message> tupleQueue;

    /**
     * BlockingQueue for acks.
     * Needs to be concurrent because it is modified by both threads.
     */
    private final Queue<String> ackQueue;

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
     * @param config configuration proeprties.
     */
    public MemoryFunnel(final ClientConfiguration config) {
        Objects.requireNonNull(config);

        tupleQueue = new LinkedBlockingQueue<>(config.getMaxTupleQueueSize());
        ackQueue = new LinkedBlockingQueue<>(config.getMaxAckQueueSize());
        failureHandler = new NoRetryHandler();
    }

    @Override
    public Message nextTuple() {
        // Should replay a failed tuple?
        Message nextMessage = failureHandler.getTuple();

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
    public boolean ackTuple(final String msgId) {
        if (msgId == null) {
            return false;
        }

        // Add to acked tuples queue
        ackQueue.offer(msgId);

        // remove from inflight tuples map.
        inFlightTuples.remove(msgId);

        return true;
    }

    @Override
    public boolean failTuple(final String msgId) {
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
        failureHandler.addFailure(failedTuple);
        return true;
    }

    @Override
    public void requestStop() {
        shouldStop.set(true);

        // Wait until stopped.
        while (!isRunning.get()) {
            try {
                Thread.sleep(2500L);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    @Override
    public boolean addTuple(final Message message) {
        return tupleQueue.offer(message);
    }

    @Override
    public String getNextAck() {
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
}
