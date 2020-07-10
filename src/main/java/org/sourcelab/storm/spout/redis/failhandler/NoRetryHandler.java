package org.sourcelab.storm.spout.redis.failhandler;

import org.sourcelab.storm.spout.redis.funnel.Message;

import java.util.Map;

/**
 * No-op implementation.
 * This FailureHandler will not replay failures at all.
 */
public class NoRetryHandler implements FailureHandler {
    @Override
    public void open(final Map<String, Object> stormConfig) {
        // Noop.
    }

    @Override
    public boolean fail(final Message object) {
        return false;
    }

    @Override
    public void ack(final String msgId) {
        // Noop
    }

    @Override
    public Message getMessage() {
        return null;
    }
}
