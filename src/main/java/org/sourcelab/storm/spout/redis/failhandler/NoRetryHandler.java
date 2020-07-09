package org.sourcelab.storm.spout.redis.failhandler;

import org.sourcelab.storm.spout.redis.funnel.Message;

public class NoRetryHandler implements FailureHandler {
    @Override
    public void addFailure(final Message object) {
        // Do nothing.
    }

    @Override
    public Message getTuple() {
        return null;
    }
}
