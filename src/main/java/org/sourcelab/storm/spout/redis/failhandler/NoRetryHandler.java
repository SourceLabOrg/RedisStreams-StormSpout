package org.sourcelab.storm.spout.redis.failhandler;

import org.sourcelab.storm.spout.redis.funnel.Message;

/**
 * No-op implementation.
 * This FailureHandler will not replay failures at all.
 */
public class NoRetryHandler implements FailureHandler {
    @Override
    public void addFailure(final Message object) {
        // Do nothing.
    }

    @Override
    public Message getMessage() {
        return null;
    }
}
