package org.sourcelab.storm.spout.redis.failhandler;

import org.apache.storm.task.TopologyContext;
import org.junit.jupiter.api.Test;
import org.sourcelab.storm.spout.redis.FailureHandler;
import org.sourcelab.storm.spout.redis.Message;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

class NoRetryHandlerTest {

    /**
     * By design this implementation does nothing....
     */
    @Test
    void smokeTest() {
        final FailureHandler handler = new NoRetryHandler();

        // Call no-op methods
        handler.open(new HashMap<>(), mock(TopologyContext.class));
        handler.ack("MsgId");

        // Create message
        final Message message = new Message("MsgId", new HashMap<>());

        // Call fail a bunch?
        for (int counter = 0; counter < 10; counter++) {
            assertFalse(handler.fail(message));
        }

        // Ask for new messages a bunch?
        for (int counter = 0; counter < 10; counter++) {
            assertNull(handler.getMessage());
        }
    }
}