package org.sourcelab.storm.spout.redis.funnel;

/**
 * Funnel from the point of the Redis Consumer.
 */
public interface ConsumerFunnel {
    boolean addTuple(final Message message);
    String getNextAck();
    boolean shouldStop();
    void setIsRunning(boolean state);
}
