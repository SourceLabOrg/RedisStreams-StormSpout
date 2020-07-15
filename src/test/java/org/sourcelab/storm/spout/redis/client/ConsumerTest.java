package org.sourcelab.storm.spout.redis.client;

import org.apache.storm.task.TopologyContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.sourcelab.storm.spout.redis.Message;
import org.sourcelab.storm.spout.redis.RedisStreamSpoutConfig;
import org.sourcelab.storm.spout.redis.example.TestTupleConverter;
import org.sourcelab.storm.spout.redis.funnel.MemoryFunnel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.time.Duration.ofSeconds;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Tests over LettuceClient using a mock RedisClient instance.
 */
class ConsumerTest {
    private static final String HOSTNAME = "my.example.host.com";
    private static final int PORT = 1234;
    private static final String STREAM_KEY = "StreamKeyValue";

    // Create config
    private final RedisStreamSpoutConfig config = RedisStreamSpoutConfig.newBuilder()
        .withServer(HOSTNAME, PORT)
        .withStreamKey(STREAM_KEY)
        .withGroupName("GroupName")
        .withConsumerIdPrefix("ConsumerId")
        .withNoRetryFailureHandler()
        .withTupleConverter(new TestTupleConverter())
        // Small delay.
        .withConsumerDelayMillis(10L)
        .build();

    private MemoryFunnel funnel;

    // Mocks
    private Client mockClient;
    private TopologyContext mockTopologyContext;

    // Instance under test
    private Consumer consumer;

    @BeforeEach
    void setup() {
        mockTopologyContext = mock(TopologyContext.class);
        funnel = new MemoryFunnel(config, new HashMap<>(), mockTopologyContext);
        mockClient = mock(Client.class);
        consumer = new Consumer(config, mockClient, funnel);


    }

    @AfterEach
    void cleanup() {
        // Ensure all interactions accounted for.
        verifyNoMoreInteractions(mockClient);
    }

    @Test
    void smokeTest() throws InterruptedException {
        // Setup return values
        final List<Message> firstMessageBatch = createMessageBatch(10, 0);
        final List<Message> secondMessageBatch = Collections.emptyList();
        final List<Message> thirdMessageBatch = createMessageBatch(20, 10);

        // All further calls just get an empty list
        final List<Message> finalBatch = Collections.emptyList();

        // Setup Mocks
        when(mockClient.nextMessages()).thenReturn(
            firstMessageBatch, secondMessageBatch, thirdMessageBatch, finalBatch
        );

        // Create a thread to run the Consumer
        final Thread consumerThread = new Thread(consumer);
        try {
            consumerThread.start();

            // Wait for startup
            assertTimeout(ofSeconds(10), () -> {
                while (!funnel.isRunning()) {
                    Thread.sleep(100L);
                }
            }, "Timed out waiting for consumer thread to start.");

            // Wait for messages to show up in the funnel
            final List<Message> receivedMessages = new ArrayList<>();
            assertTimeout(ofSeconds(10), () -> {
                while (receivedMessages.size() < 30) {
                    final Message nextMessage = funnel.nextMessage();
                    if (nextMessage != null) {
                        receivedMessages.add(nextMessage);
                    }
                }
            }, "Timed out waiting to receive messages");

            // Verify we got all the expected messages
            assertEquals(30, receivedMessages.size(), "Should have gotten 30 messages.");

            // Verify them
            for (int index = 0; index < receivedMessages.size(); index++) {
                final String foundId = receivedMessages.get(index).getId();
                final String expectedId = "Id" + index;
                assertEquals(expectedId, foundId);
            }

            // Call next message on funnel, it should have nothing
            for (int counter = 0; counter < 10; counter++) {
                assertNull(funnel.nextMessage(), "No more messages should be available");
                Thread.sleep(100L);
            }

            // Now lets ack each message
            receivedMessages.forEach((msg) -> funnel.ackMessage(msg.getId()));

            // Wait a bit
            Thread.sleep(3000L);

            // Then request shutdown
            funnel.requestStop();

            // Wait for thread to stop.
            assertTimeout(ofSeconds(10), (Executable) consumerThread::join, "Thread never stopped!");

            // Verify interactions
            verify(mockClient, times(1))
                .connect();

            // Received call to nextMessage at least 4 times
            verify(mockClient, atLeast(4)).nextMessages();

            // Received ack for each msg once
            receivedMessages.forEach((msg) -> {
                verify(mockClient, times(1)).commitMessage(eq(msg.getId()));
            });

            // Verify shutdown call
            verify(mockClient, times(1)).disconnect();

            // Verify funnel updated with status
            assertFalse(funnel.isRunning());
        } finally {
            if (consumerThread.isAlive()) {
                funnel.requestStop();
                consumerThread.interrupt();
                consumerThread.join(3000L);
            }
        }
    }

    private List<Message> createMessageBatch(final int count, int startingValue) {
        final List<Message> messages = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            final String messageId = "Id" + startingValue;
            final Map<String, String> values = new HashMap<>();
            values.put("Key", "Value" + startingValue);

            messages.add(new Message(messageId, values));

            startingValue++;
        }

        return messages;
    }
}