package org.sourcelab.storm.spout.redis.failhandler;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import org.apache.storm.task.TopologyContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sourcelab.storm.spout.redis.Message;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class ExponentialBackoffFailureHandlerTest {
    /**
     * Used to mock the system clock.
     */
    private static final long FIXED_TIME = 100000L;
    private Clock mockClock;
    private TopologyContext mockTopologyContext;

    /**
     * Handles mocking Clock using Java 1.8's Clock interface.
     */
    @BeforeEach
    public void setup() {
        // Set our clock to be fixed.
        mockClock = Clock.fixed(Instant.ofEpochMilli(FIXED_TIME), ZoneId.of("UTC"));
        mockTopologyContext = mock(TopologyContext.class);

        // Setup mock to return a non-null counter when called.
        when(mockTopologyContext.registerCounter(anyString()))
            .thenReturn(new Counter());
    }

    @AfterEach
    public void cleanup() {
        // Verify mock interactions
        verifyNoMoreInteractions(mockTopologyContext);
    }

    /**
     * Tests tracking a new failed messages.
     */
    @Test
    public void testFailedSimpleCase() {
        final int expectedMaxRetries = 10;
        final long expectedMinRetryTimeMs = 1000;
        final double expectedDelayMultiplier = 44.5;

        // Build config
        final ExponentialBackoffConfig config = ExponentialBackoffConfig.newBuilder()
            .withRetryDelayMultiplier(expectedDelayMultiplier)
            .withRetryLimit(expectedMaxRetries)
            .withInitialRetryDelayMs(expectedMinRetryTimeMs)
            .build();


        // Create instance, inject our mock clock,  and call open.
        final ExponentialBackoffFailureHandler handler = new ExponentialBackoffFailureHandler(config);
        handler.open(new HashMap<>(), mockTopologyContext);
        handler.setClock(mockClock);

        // Calculate the 1st retry times
        final long firstRetryTime = FIXED_TIME + (long) (expectedMinRetryTimeMs * Math.pow(expectedDelayMultiplier, 0));

        // Define our messages
        final Message message1 = new Message("msgId1", new HashMap<>());
        final Message message2 = new Message("msgId2", new HashMap<>());
        final Message message3 = new Message("msgId3", new HashMap<>());

        // Mark first as having failed
        handler.fail(message1);

        // Validate it has failed
        validateExpectedFailedMessage(handler, message1, 1, firstRetryTime);

        // Mark second as having failed
        handler.fail(message2);

        // Validate it has first two as failed
        validateExpectedFailedMessage(handler, message1, 1, firstRetryTime);
        validateExpectedFailedMessage(handler, message2, 1, firstRetryTime);

        // Mark 3rd as having failed
        handler.fail(message3);

        // Validate it has all three as failed
        validateExpectedFailedMessage(handler, message1, 1, firstRetryTime);
        validateExpectedFailedMessage(handler, message2, 1, firstRetryTime);
        validateExpectedFailedMessage(handler, message3, 1, firstRetryTime);

        // Verify metric interactions
        verifyMetricInteractions();
    }

    /**
     * Tests tracking a new failed messageId.
     */
    @Test
    public void testFailedMultipleFails() {
        final int expectedMaxRetries = 10;
        final long expectedMinRetryTimeMs = 1000;
        final double expectedDelayMultiplier = 7.25;

        // Build config
        final ExponentialBackoffConfig config = ExponentialBackoffConfig.newBuilder()
            .withRetryDelayMultiplier(expectedDelayMultiplier)
            .withRetryLimit(expectedMaxRetries)
            .withInitialRetryDelayMs(expectedMinRetryTimeMs)
            .build();


        // Create instance, inject our mock clock,  and call open.
        final ExponentialBackoffFailureHandler handler = new ExponentialBackoffFailureHandler(config);
        handler.open(new HashMap<>(), mockTopologyContext);
        handler.setClock(mockClock);

        // Define our messages
        final Message message1 = new Message("msgId1", new HashMap<>());
        final Message message2 = new Message("msgId2", new HashMap<>());

        // Calculate the 1st, 2nd, and 3rd fail retry times
        final long firstRetryTime = FIXED_TIME + (long) (expectedMinRetryTimeMs * Math.pow(expectedDelayMultiplier, 0));
        final long secondRetryTime = FIXED_TIME + (long) (expectedMinRetryTimeMs * Math.pow(expectedDelayMultiplier, 1));
        final long thirdRetryTime = FIXED_TIME + (long) (expectedMinRetryTimeMs * Math.pow(expectedDelayMultiplier, 2));

        // Mark first as having failed
        handler.fail(message1);

        // Validate it has failed
        validateExpectedFailedMessage(handler, message1, 1, firstRetryTime);

        // Mark second as having failed
        handler.fail(message2);

        // Validate it has first two as failed
        validateExpectedFailedMessage(handler, message1, 1, firstRetryTime);
        validateExpectedFailedMessage(handler, message2, 1, firstRetryTime);

        // Now fail messageId1 a second time.
        handler.fail(message1);

        // Validate it has first two as failed
        validateExpectedFailedMessage(handler, message1, 2, secondRetryTime);
        validateExpectedFailedMessage(handler, message2, 1, firstRetryTime);

        // Now fail messageId1 a 3rd time.
        handler.fail(message1);

        // Validate it has first two as failed
        validateExpectedFailedMessage(handler, message1, 3, thirdRetryTime);
        validateExpectedFailedMessage(handler, message2, 1, firstRetryTime);

        // Now fail messageId2 a 2nd time.
        handler.fail(message2);

        // Validate it has first two as failed
        validateExpectedFailedMessage(handler, message1, 3, thirdRetryTime);
        validateExpectedFailedMessage(handler, message2, 2, secondRetryTime);

        // Verify metric interactions
        verifyMetricInteractions();
    }

    /**
     * Tests what happens if a tuple fails more than our max fail limit.
     */
    @Test
    public void testRetryDelayWhenExceedsMaxTimeDelaySetting() {
        final int expectedMaxRetries = 10;
        final long expectedMinRetryTimeMs = 1000;
        final double expectedDelayMultiplier = 10;

        // Set a max delay of 12 seconds
        final long expectedMaxDelay = 12000;

        // Build config
        final ExponentialBackoffConfig config = ExponentialBackoffConfig.newBuilder()
            .withRetryDelayMultiplier(expectedDelayMultiplier)
            .withRetryLimit(expectedMaxRetries)
            .withInitialRetryDelayMs(expectedMinRetryTimeMs)
            .withRetryDelayMaxMs(expectedMaxDelay)
            .build();

        // Create instance, inject our mock clock,  and call open.
        final ExponentialBackoffFailureHandler handler = new ExponentialBackoffFailureHandler(config);
        handler.open(new HashMap<>(), mockTopologyContext);
        handler.setClock(mockClock);

        // Define our messages
        final Message message1 = new Message("msgId1", new HashMap<>());
        
        // Calculate the 1st, 2nd, and 3rd fail retry times
        final long firstRetryTime = FIXED_TIME + (long) (expectedMinRetryTimeMs * Math.pow(expectedDelayMultiplier, 0)); // 1 second
        final long secondRetryTime = FIXED_TIME + (long) (expectedMinRetryTimeMs * Math.pow(expectedDelayMultiplier, 1)); // 10 seconds
        final long thirdRetryTime = FIXED_TIME + expectedMaxDelay;

        // Mark first as having failed
        handler.fail(message1);

        // Validate it has failed
        validateExpectedFailedMessage(handler, message1, 1, firstRetryTime);

        // Now fail message1 a second time.
        handler.fail(message1);

        // Validate it incremented our delay time, still below configured max delay
        validateExpectedFailedMessage(handler, message1, 2, secondRetryTime);

        // Now fail message1 a 3rd time.
        handler.fail(message1);

        // Validate its pinned at configured max delay
        validateExpectedFailedMessage(handler, message1, 3, thirdRetryTime);

        // Now fail message1 a 4th time.
        handler.fail(message1);

        // Validate its still pinned at configured max delay
        validateExpectedFailedMessage(handler, message1, 4, thirdRetryTime);

        // Verify metric interactions
        verifyMetricInteractions();
    }

    /**
     * Tests fail() always returns false if configured to never retry.
     */
    @Test
    public void testFailWithMaxRetriesSetToZero() {
        final long expectedMinRetryTimeMs = 1000;
        final double expectedDelayMultiplier = 10;

        // Build config
        final ExponentialBackoffConfig config = ExponentialBackoffConfig.newBuilder()
            .withRetryNever()
            .withRetryDelayMultiplier(expectedDelayMultiplier)
            .withInitialRetryDelayMs(expectedMinRetryTimeMs)
            .build();

        // Create instance, inject our mock clock, and call open.
        final ExponentialBackoffFailureHandler handler = new ExponentialBackoffFailureHandler(config);
        handler.open(new HashMap<>(), mockTopologyContext);
        handler.setClock(mockClock);

        // Define our messages
        final Message message1 = new Message("msgId1", new HashMap<>());

        for (int x = 0; x < 100; x++) {
            assertFalse(handler.fail(message1), "Should always be false because we are configured to never retry");
        }

        // Verify metric interactions
        verifyMetricInteractions();
    }

    /**
     * Tests fail always returns true if maxRetries is configured to a value less than 0.
     */
    @Test
    public void testRetryForever() {
        // construct manager
        final int expectedMaxRetries = -1;
        final long expectedMinRetryTimeMs = 1000;
        final double expectedDelayMultiplier = 10;

        // Build config
        final ExponentialBackoffConfig config = ExponentialBackoffConfig.newBuilder()
            .withRetryForever()
            .withRetryDelayMultiplier(expectedDelayMultiplier)
            .withInitialRetryDelayMs(expectedMinRetryTimeMs)
            .build();

        // Create instance, inject our mock clock,  and call open.
        final ExponentialBackoffFailureHandler handler = new ExponentialBackoffFailureHandler(config);
        handler.open(new HashMap<>(), mockTopologyContext);
        handler.setClock(mockClock);

        // Define our messages
        final Message message1 = new Message("msgId1", new HashMap<>());

        for (int x = 0;  x < 100; x++) {
            // See if accepted by handler
            assertTrue(handler.fail(message1), "Should always be true because we are configured to always retry");
        }

        // Verify metric interactions
        verifyMetricInteractions();
    }

    /**
     * Tests what happens if a tuple fails more than our max fail limit.
     */
    @Test
    public void testRetryFurtherWhenMessageExceedsRetryLimit() {
        // construct manager
        final int expectedMaxRetries = 3;
        final long expectedMinRetryTimeMs = 1000;
        final double expectedDelayMultiplier = 1.5;

        // Build config
        final ExponentialBackoffConfig config = ExponentialBackoffConfig.newBuilder()
            .withRetryLimit(expectedMaxRetries)
            .withRetryDelayMultiplier(expectedDelayMultiplier)
            .withInitialRetryDelayMs(expectedMinRetryTimeMs)
            .build();

        // Create instance, inject our mock clock,  and call open.
        final ExponentialBackoffFailureHandler handler = new ExponentialBackoffFailureHandler(config);
        handler.open(new HashMap<>(), mockTopologyContext);
        handler.setClock(mockClock);

        // Define our messages
        final Message message1 = new Message("msgId1", new HashMap<>());
        final Message message2 = new Message("msgId2", new HashMap<>());

        // Calculate the 1st, 2nd, and 3rd fail retry times
        final long firstRetryTime = FIXED_TIME + (long) (expectedMinRetryTimeMs * Math.pow(expectedDelayMultiplier, 0));
        final long secondRetryTime = FIXED_TIME + (long) (expectedMinRetryTimeMs * Math.pow(expectedDelayMultiplier, 1));
        final long thirdRetryTime = FIXED_TIME + (long) (expectedMinRetryTimeMs * Math.pow(expectedDelayMultiplier, 2));

        // Mark first as having failed
        handler.fail(message1);

        // Validate it has failed
        validateExpectedFailedMessage(handler, message1, 1, firstRetryTime);

        // Mark second as having failed
        handler.fail(message2);

        // Validate it has first two as failed
        validateExpectedFailedMessage(handler, message1, 1, firstRetryTime);
        validateExpectedFailedMessage(handler, message2, 1, firstRetryTime);

        // Fail message1 a 2nd time
        assertTrue(handler.fail(message1), "Should be accepted");

        // Validate it has first two as failed
        validateExpectedFailedMessage(handler, message1, 2, secondRetryTime);
        validateExpectedFailedMessage(handler, message2, 1, firstRetryTime);

        // Fail message 1 a 3rd time
        assertTrue(handler.fail(message1), "Should be accepted");

        // Validate it has first two as failed
        validateExpectedFailedMessage(handler, message1, 3, thirdRetryTime);
        validateExpectedFailedMessage(handler, message2, 1, firstRetryTime);

        // Validate that message1 is not accepted again.
        assertFalse(handler.fail(message1), "Should NOT be able to retry");
        assertTrue(handler.fail(message2), "Should be able to retry");

        // Verify metric interactions
        verifyMetricInteractions();
    }

    /**
     * Tests calling nextFailedMessageToRetry() when nothing should have been expired.
     */
    @Test
    public void testNextFailedMessageToRetryWithNothingExpired() {
        // construct manager
        final int expectedMaxRetries = 3;
        final long expectedMinRetryTimeMs = 1000;
        final double expectedDelayMultiplier = 4.2;

        // Build config
        final ExponentialBackoffConfig config = ExponentialBackoffConfig.newBuilder()
            .withRetryLimit(expectedMaxRetries)
            .withRetryDelayMultiplier(expectedDelayMultiplier)
            .withInitialRetryDelayMs(expectedMinRetryTimeMs)
            .build();

        // Create instance, inject our mock clock,  and call open.
        final ExponentialBackoffFailureHandler handler = new ExponentialBackoffFailureHandler(config);
        handler.open(new HashMap<>(), mockTopologyContext);
        handler.setClock(mockClock);

        // Define our messages
        final Message message1 = new Message("msgId1", new HashMap<>());
        final Message message2 = new Message("msgId2", new HashMap<>());

        // Mark both as having been failed.
        handler.fail(message1);
        handler.fail(message2);

        // Validate it has first two as failed
        validateExpectedFailedMessage(
            handler,
            message1,
            1,
            (FIXED_TIME + (long) expectedMinRetryTimeMs)
        );
        validateExpectedFailedMessage(
            handler,
            message2,
            1,
            (FIXED_TIME + (long) expectedMinRetryTimeMs)
        );

        // Ask for the next tuple to retry, should be empty
        assertNull(handler.getMessage(), "Should be null");
        assertNull(handler.getMessage(), "Should be null");
        assertNull(handler.getMessage(), "Should be null");
        assertNull(handler.getMessage(), "Should be null");

        // Verify metric interactions
        verifyMetricInteractions();
    }

    /**
     * Tests calling nextFailedMessageToRetry() when nothing should have been expired.
     */
    @Test
    public void testNextFailedMessageToRetryWithExpiring() {
        // construct manager
        final int expectedMaxRetries = 3;
        final long expectedMinRetryTimeMs = 1000;
        final double expectedDelayMultiplier = 6.2;

        // Build config
        final ExponentialBackoffConfig config = ExponentialBackoffConfig.newBuilder()
            .withRetryLimit(expectedMaxRetries)
            .withRetryDelayMultiplier(expectedDelayMultiplier)
            .withInitialRetryDelayMs(expectedMinRetryTimeMs)
            .build();

        // Create instance, inject our mock clock,  and call open.
        final ExponentialBackoffFailureHandler handler = new ExponentialBackoffFailureHandler(config);
        handler.open(new HashMap<>(), mockTopologyContext);
        handler.setClock(mockClock);

        // Define our messages
        final Message message1 = new Message("msgId1", new HashMap<>());
        final Message message2 = new Message("msgId2", new HashMap<>());

        // Mark message1 as having failed
        handler.fail(message1);

        // Mark message2 as having failed twice
        handler.fail(message2);
        handler.fail(message2);

        // Calculate the first and 2nd fail retry times
        final long firstRetryTime = FIXED_TIME + (long) (expectedMinRetryTimeMs * Math.pow(expectedDelayMultiplier, 0));
        final long secondRetryTime = FIXED_TIME + (long) (expectedMinRetryTimeMs * Math.pow(expectedDelayMultiplier, 1));

        // Validate it has first two as failed
        validateExpectedFailedMessage(handler, message1, 1, firstRetryTime);
        validateExpectedFailedMessage(handler, message2, 2, secondRetryTime);

        // Ask for the next tuple to retry, should be empty
        assertNull(handler.getMessage(), "Should be null");
        assertNull(handler.getMessage(), "Should be null");
        assertNull(handler.getMessage(), "Should be null");
        assertNull(handler.getMessage(), "Should be null");

        // Now advance time by exactly expectedMinRetryTimeMs milliseconds
        handler.setClock(
            Clock.fixed(
                Instant.ofEpochMilli(FIXED_TIME + (long) (expectedMinRetryTimeMs * Math.pow(expectedDelayMultiplier, 0))),
                ZoneOffset.UTC
            ));

        // Now message1 should expire next,
        Message nextMessageToBeRetried = handler.getMessage();
        assertNotNull(nextMessageToBeRetried, "result should not be null");
        assertEquals(message1.getId(), nextMessageToBeRetried.getId(), "Should be our message1");

        // Validate the internal state.
        validateTupleNotInFailedSet(handler, message1);
        validateExpectedFailedMessage(handler, message2, 2, secondRetryTime);

        // Calling nextFailedMessageToRetry should result in null.
        assertNull(handler.getMessage(), "Should be null");
        assertNull(handler.getMessage(), "Should be null");
        assertNull(handler.getMessage(), "Should be null");
        assertNull(handler.getMessage(), "Should be null");

        // Advance time again, by expected retry time, plus a few MS
        final long newFixedTime = FIXED_TIME + (long) (expectedMinRetryTimeMs * Math.pow(expectedDelayMultiplier, 1)) + 10;
        handler.setClock(Clock.fixed(Instant.ofEpochMilli(newFixedTime), ZoneId.of("UTC")));

        // Now message1 should expire next,
        nextMessageToBeRetried = handler.getMessage();
        assertNotNull(nextMessageToBeRetried, "result should not be null");

        // Validate state.
        validateTupleNotInFailedSet(handler, message1);
        validateTupleNotInFailedSet(handler, message2);

        // call ack, validate its no longer tracked
        handler.ack(message1.getId());
        validateTupleIsNotBeingTracked(handler, message1);
        validateTupleNotInFailedSet(handler, message2);

        // Calculate retry time for 3rd fail against new fixed time
        final long thirdRetryTime = newFixedTime + (long) (expectedMinRetryTimeMs * Math.pow(expectedDelayMultiplier, 2));

        // Mark tuple2 as having failed
        handler.fail(message2);
        validateTupleIsNotBeingTracked(handler, message1);
        validateExpectedFailedMessage(handler, message2, 3, thirdRetryTime);

        // Verify metric interactions
        verifyMetricInteractions();
    }

    /**
     * Validates that when we have multiple failed tuples that need to be retried,
     * we retry the earliest ones first.
     */
    @Test
    public void testRetryEarliestFailed() {
        // construct manager
        final int expectedMaxRetries = 3;
        final long expectedMinRetryTimeMs = 0;
        final double expectedDelayMultiplier = 0.5;

        // Build config
        final ExponentialBackoffConfig config = ExponentialBackoffConfig.newBuilder()
            .withRetryLimit(expectedMaxRetries)
            .withRetryDelayMultiplier(expectedDelayMultiplier)
            .withInitialRetryDelayMs(expectedMinRetryTimeMs)
            .build();

        // Create instance, inject our mock clock,  and call open.
        final ExponentialBackoffFailureHandler handler = new ExponentialBackoffFailureHandler(config);
        handler.open(new HashMap<>(), mockTopologyContext);
        handler.setClock(mockClock);

        // Define our messages
        final Message message1 = new Message("msgId1", new HashMap<>());
        final Message message2 = new Message("msgId2", new HashMap<>());
        final Message message3 = new Message("msgId3", new HashMap<>());

        // Fail messageId 1 @ T0
        handler.fail(message1);

        // Increment clock to T1 and fail messageId 2
        handler.setClock(Clock.fixed(Instant.ofEpochMilli(FIXED_TIME + 100), ZoneId.of("UTC")));
        handler.fail(message2);

        // Increment clock to T2 and fail messageId 3
        handler.setClock(Clock.fixed(Instant.ofEpochMilli(FIXED_TIME + 200), ZoneId.of("UTC")));
        handler.fail(message3);

        // call getMessage() 3 times
        // We'd expect to get message1 since its the oldest, followed by message2, and then message3
        final Message result1 = handler.getMessage();
        final Message result2 = handler.getMessage();
        final Message result3 = handler.getMessage();

        assertEquals(message1.getId(), result1.getId(), "Result1 should be message1");
        assertEquals(message2.getId(), result2.getId(), "Result2 should be message1");
        assertEquals(message3.getId(), result3.getId(), "Result3 should be message1");

        // Future calls should be null
        assertNull(handler.getMessage());
        assertNull(handler.getMessage());
        assertNull(handler.getMessage());

        // Verify metric interactions
        verifyMetricInteractions();
    }

    private void validateExpectedFailedMessage(
        final ExponentialBackoffFailureHandler handler,
        final Message message,
        final int expectedFailCount,
        final long expectedRetryTime
    ) {
        final String messageId = message.getId();

        // Find its queue
        final Queue<Message> failQueue = handler.getFailedMessages().get(expectedRetryTime);
        assertNotNull(
            failQueue,
            "Queue should exist for our retry time of " + expectedRetryTime + " has [" + handler.getFailedMessages().keySet() + "]"
        );
        assertTrue(failQueue.contains(message), "Queue should contain our message");

        // This messageId should have the right number of fails associated with it.
        assertEquals(
            (Integer) expectedFailCount,
            handler.getNumberOfTimesFailed().get(messageId),
            "Should have expected number of fails"
        );
    }

    private void validateTupleNotInFailedSet(final ExponentialBackoffFailureHandler handler, final Message message) {
        // Loop through all failed tuples
        for (final Long key : handler.getFailedMessages().keySet()) {
            final Queue queue = handler.getFailedMessages().get(key);
            assertFalse(queue.contains(message), "Should not contain our message");
        }
    }

    private void validateTupleIsNotBeingTracked(final ExponentialBackoffFailureHandler handler, final Message message) {
        // Loop through all failed tuples
        for (final Long key : handler.getFailedMessages().keySet()) {
            final Queue queue = handler.getFailedMessages().get(key);
            assertFalse(queue.contains(message), "Should not contain our message");
        }
        assertFalse(handler.getNumberOfTimesFailed().containsKey(message), "Should not have a fail count");
    }

    private void verifyMetricInteractions() {
        verify(mockTopologyContext, times(1))
            .registerCounter(eq("failureHandler_exceededRetryLimit"));
        verify(mockTopologyContext, times(1))
            .registerCounter(eq("failureHandler_retriedMessages"));
        verify(mockTopologyContext, times(1))
            .registerCounter(eq("failureHandler_successfulRetriedMessages"));
        verify(mockTopologyContext, times(1))
            .registerGauge(eq("failureHandler_queuedForRetry"), any(Gauge.class));
    }
}