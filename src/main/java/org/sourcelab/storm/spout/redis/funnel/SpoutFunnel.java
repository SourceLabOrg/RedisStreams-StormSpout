package org.sourcelab.storm.spout.redis.funnel;

/**
 * Funnel from Spouts point of view.
 */
public interface SpoutFunnel {
    Message nextTuple();
    boolean ackTuple(final String msgId);
    boolean failTuple(final String msgId);
    void requestStop();
}
