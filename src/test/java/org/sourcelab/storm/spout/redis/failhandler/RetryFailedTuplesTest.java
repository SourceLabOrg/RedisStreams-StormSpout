package org.sourcelab.storm.spout.redis.failhandler;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.sourcelab.storm.spout.redis.Message;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RetryFailedTuplesTest {

    /**
     * Verify that messages are retried up to the maximum configured limit.
     * In this case we set a limit of 2 retries.
     */
    @Test
    void verify_noReplaysAfterMaxReached() {
        final int maxLimit = 2;

        // Create test message
        final String msgId = "MyMsgId1";
        final Map<String, String> body = Collections.singletonMap("MyKey", "MyValue");
        final Message message = new Message(msgId, body);

        // Configure with 2 retries
        final Map<String, Object> stormConfig = new HashMap<>();

        // Create instance
        final RetryFailedTuples handler = new RetryFailedTuples(maxLimit);
        handler.open(stormConfig);

        // If we ask for the next message, it should return null
        assertNull(handler.getMessage(), "Should have no msgs");
        assertNull(handler.getMessage(), "Should have no msgs");

        // Call fail with our message
        // 1st fail
        boolean result = handler.fail(message);
        assertTrue(result, "Handler should have accepted the message");

        // If we ask for the next message, it should give us back our failed messaged.
        Message returnedMessage = handler.getMessage();
        assertNotNull(returnedMessage, "Should have given us our message back");
        assertEquals(msgId, returnedMessage.getId());

        // If we ask for more msgs, it should return null.
        assertNull(handler.getMessage(), "Should have no msgs");
        assertNull(handler.getMessage(), "Should have no msgs");

        // If we fail this msg again, it should accept it
        // 2nd fail
        result = handler.fail(message);
        assertTrue(result, "Handler should have accepted the message");

        // If we ask for the next message, it should give us back our failed messaged.
        returnedMessage = handler.getMessage();
        assertNotNull(returnedMessage, "Should have given us our message back");
        assertEquals(msgId, returnedMessage.getId());

        // If we ask for more msgs, it should return null.
        assertNull(handler.getMessage(), "Should have no msgs");
        assertNull(handler.getMessage(), "Should have no msgs");

        // If we fail this message again, it should reject it
        // Third fail.
        result = handler.fail(message);
        assertFalse(result, "Should have rejected our fail.");

        // If we ask for more msgs, it should return null.
        assertNull(handler.getMessage(), "Should have no msgs");
        assertNull(handler.getMessage(), "Should have no msgs");
    }

    /**
     * Verify that messages are endlessly replayed if configured
     * to a max of -1.
     */
    @ParameterizedTest
    @MethodSource("provideNegativeValues")
    void verify_alwaysReplay(final Integer cfgValue) {
        // Create test message
        final String msgId = "MyMsgId1";
        final Map<String, String> body = Collections.singletonMap("MyKey", "MyValue");
        final Message message = new Message(msgId, body);

        // Configure with -1 retries (always retry)
        final Map<String, Object> stormConfig = new HashMap<>();

        // Create instance
        final RetryFailedTuples handler = new RetryFailedTuples(cfgValue);
        handler.open(stormConfig);

        // If we ask for the next message, it should return null
        assertNull(handler.getMessage(), "Should have no msgs");
        assertNull(handler.getMessage(), "Should have no msgs");

        for (int loopCount = 0; loopCount < 64; loopCount++) {
            // Call fail with our message
            // 1st fail
            boolean result = handler.fail(message);
            assertTrue(result, "Handler should have accepted the message");

            // If we ask for the next message, it should give us back our failed messaged.
            Message returnedMessage = handler.getMessage();
            assertNotNull(returnedMessage, "Should have given us our message back");
            assertEquals(msgId, returnedMessage.getId());

            // If we ask for more msgs, it should return null.
            assertNull(handler.getMessage(), "Should have no msgs");
            assertNull(handler.getMessage(), "Should have no msgs");
        }

        // Lets ack the message
        handler.ack(msgId);

        // If we ask for the next message we should receive null.
        assertNull(handler.getMessage(), "Should have no msgs");
        assertNull(handler.getMessage(), "Should have no msgs");
    }

    /**
     * Verify that messages are endlessly replayed if configured
     * to a max of 0.
     */
    @Test
    void verify_neverReplay() {
        final int maxLimit = 0;

        // Create test message
        final String msgId = "MyMsgId1";
        final Map<String, String> body = Collections.singletonMap("MyKey", "MyValue");
        final Message message = new Message(msgId, body);

        // Configure with -1 retries (always retry)
        final Map<String, Object> stormConfig = new HashMap<>();

        // Create instance
        final RetryFailedTuples handler = new RetryFailedTuples(maxLimit);
        handler.open(stormConfig);

        // If we ask for the next message, it should return null
        assertNull(handler.getMessage(), "Should have no msgs");
        assertNull(handler.getMessage(), "Should have no msgs");

        for (int loopCount = 0; loopCount < 64; loopCount++) {
            // Call fail with our message
            // 1st fail
            boolean result = handler.fail(message);
            assertFalse(result, "Handler should not accept the message");

            // If we ask for more msgs, it should return null.
            assertNull(handler.getMessage(), "Should have no msgs");
            assertNull(handler.getMessage(), "Should have no msgs");
        }

        // Lets ack the message
        handler.ack(msgId);

        // If we ask for the next message we should receive null.
        assertNull(handler.getMessage(), "Should have no msgs");
        assertNull(handler.getMessage(), "Should have no msgs");
    }

    static Stream<Arguments> provideNegativeValues() {
        return Stream.of(
            // Integer value
            Arguments.of(-1),
            // String value
            Arguments.of(-2)
        );
    }
}