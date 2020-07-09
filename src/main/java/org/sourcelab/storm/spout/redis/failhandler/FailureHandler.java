package org.sourcelab.storm.spout.redis.failhandler;

import org.sourcelab.storm.spout.redis.funnel.Message;

/**
 * For handling failed tuples.
 * Does NOT need to be thread safe as only accessed via a single thread.
 */
public interface FailureHandler {
    void addFailure(final Message object);
    Message getTuple();
}
