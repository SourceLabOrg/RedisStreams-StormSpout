package org.sourcelab.storm.spout.redis;

import org.apache.storm.spout.ISpout;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.sourcelab.storm.spout.redis.failhandler.NoRetryHandler;
import org.sourcelab.storm.spout.redis.util.test.RedisTestHelper;
import org.sourcelab.storm.spout.redis.util.outputcollector.EmittedTuple;
import org.sourcelab.storm.spout.redis.util.outputcollector.StubSpoutCollector;
import org.sourcelab.storm.spout.redis.util.test.TestTupleConverter;
import org.sourcelab.storm.spout.redis.util.test.StreamConsumerInfo;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Integration Test.
 */
@Testcontainers
@Tag("Integration")
class RedisStreamSpoutIntegrationTest {
    /**
     * This test depends ont he following Redis Container.
     */
    @Container
    public GenericContainer redis = new GenericContainer<>("redis:5.0.3-alpine")
        .withExposedPorts(6379);

    // Configuration values
    private static final String GROUP_NAME = "MyGroupName";
    private static final String CONSUMER_ID_PREFIX = "ConsumerIdPrefix";
    private static final String CONSUMER_ID = CONSUMER_ID_PREFIX + "2";
    private static final String FAILURE_HANDLER_CLASS = NoRetryHandler.class.getName();
    private static final String TUPLE_CONVERTER_CLASS = TestTupleConverter.class.getName();

    private final Map<String, Object> stormConfig = Collections.emptyMap();

    private RedisTestHelper redisTestHelper;
    private RedisStreamSpoutConfig.Builder configBuilder;
    private String streamKey;

    // Mocks
    private TopologyContext mockTopologyContext;

    @BeforeEach
    void setup() {
        // Generate a random stream key
        streamKey = "MyStreamKey" + System.currentTimeMillis();

        // Create config
        configBuilder = RedisStreamSpoutConfig.newBuilder()
            // Set Connection Properties
            .withHost(redis.getHost())
            .withPort(redis.getFirstMappedPort())
            // Consumer Properties
            .withGroupName(GROUP_NAME)
            .withConsumerIdPrefix(CONSUMER_ID_PREFIX)
            .withStreamKey(streamKey)
            // Failure Handler
            .withNoRetryFailureHandler()
            // Tuple Handler Class
            .withTupleConverter(new TestTupleConverter());

        // Setup mock
        mockTopologyContext = mock(TopologyContext.class);
        when(mockTopologyContext.getThisTaskIndex())
            .thenReturn(2);

        // Create test helper
        redisTestHelper = new RedisTestHelper("redis://" + redis.getHost() + ":" + redis.getFirstMappedPort());
    }

    @AfterEach
    void cleanup() {
        // Verify all mock interactions accounted for
        verifyNoMoreInteractions(mockTopologyContext);
    }

    /**
     * Most basic lifecycle smoke test.
     */
    @Test
    void smokeTest_openAndClose() {
        // Create spout
        final ISpout spout = new RedisStreamSpout(configBuilder.build());
        final StubSpoutCollector collector = new StubSpoutCollector();

        // Open spout
        spout.open(stormConfig, mockTopologyContext, new SpoutOutputCollector(collector));

        // Close spout
        spout.close();

        // Verify mocks
        verify(mockTopologyContext, times(1)).getThisTaskIndex();
    }

    /**
     * Basic lifecycle smoke test.
     */
    @Test
    void smokeTest_openActivateDeactivateAndClose() throws InterruptedException {
        // Create spout
        final ISpout spout = new RedisStreamSpout(configBuilder.build());
        final StubSpoutCollector collector = new StubSpoutCollector();

        // Open spout
        spout.open(stormConfig, mockTopologyContext, new SpoutOutputCollector(collector));

        // activate spout
        spout.activate();

        // Small sleep
        Thread.sleep(3000L);

        // Deactivate (noop)
        spout.deactivate();

        // Close spout
        spout.close();

        // Verify mocks
        verify(mockTopologyContext, times(1)).getThisTaskIndex();
    }

    /**
     * Verifies the behavior when you attempt to connect to a redis instance
     * that does not exist.  Looks like nothing. You get errors in the logs.
     *
     * Disabled for now.
     */
    void smokeTest_configureInvalidRedisHost() throws InterruptedException {
        // Lets override the redis host with something invalid
        configBuilder.withPort(1234);

        // Create spout
        final ISpout spout = new RedisStreamSpout(configBuilder.build());
        final StubSpoutCollector collector = new StubSpoutCollector();

        // Open spout
        spout.open(stormConfig, mockTopologyContext, new SpoutOutputCollector(collector));

        // activate spout
        spout.activate();

        // Small sleep
        Thread.sleep(3000L);

        // Deactivate (noop)
        spout.deactivate();

        // Lets try calling activate one more time
        spout.activate();
        spout.deactivate();

        // Close spout
        spout.close();

        // Verify mocks
        verify(mockTopologyContext, times(1)).getThisTaskIndex();
    }

    /**
     * Basic lifecycle smoke test.
     */
    @Test
    void smokeTest_consumeAndAckMessages() throws InterruptedException {
        // Create spout
        final ISpout spout = new RedisStreamSpout(configBuilder.build());
        final StubSpoutCollector collector = new StubSpoutCollector();

        // Open spout
        spout.open(stormConfig, mockTopologyContext, new SpoutOutputCollector(collector));

        // activate spout
        spout.activate();

        // Lets publish 10 messages to the stream
        final List<String> producedMsgIds = redisTestHelper.produceMessages(streamKey, 10);

        // Now lets try to get those from the spout
        do {
            spout.nextTuple();
            Thread.sleep(100L);
        } while (collector.getEmittedTuples().size() < 10);

        // Call next tuple a few more times, should be a no-op
        for (int counter = 0; counter < 10; counter++) {
            Thread.sleep(100L);
            spout.nextTuple();
        }

        // Verify what got emitted.
        assertEquals(10, collector.getEmittedTuples().size(), "Should have found 10 emitted tuples.");

        final String expectedStreamId = Utils.DEFAULT_STREAM_ID;
        for (int index = 0; index < producedMsgIds.size(); index++) {
            final EmittedTuple emittedTuple = collector.getEmittedTuples().get(index);

            // Verify message Id.
            assertEquals(producedMsgIds.get(index), emittedTuple.getMessageId());

            // Verify Stream Id
            assertEquals(expectedStreamId, emittedTuple.getStreamId());

            // Verify tuple value
            assertEquals(3, emittedTuple.getTuple().size(), "Should have 3 values");

            // Look for value
            final String expectedKeyValue = "key" + index;
            boolean foundValue = emittedTuple.getTuple().stream()
                .anyMatch((entry) -> entry.equals(expectedKeyValue));
            assertTrue(foundValue, "Failed to find key tuple value");

            final String expectedMsgIdValue = producedMsgIds.get(index);
            foundValue = emittedTuple.getTuple().stream()
                .anyMatch((entry) -> entry.equals(expectedMsgIdValue));
            assertTrue(foundValue, "Failed to find msgId tuple value");
        }

        // See that we have 10 items pending
        StreamConsumerInfo consumerInfo = redisTestHelper.getStreamInfo(streamKey, GROUP_NAME, CONSUMER_ID);
        assertNotNull(consumerInfo, "Failed to find consumer info!");

        // Verify we have 10 items pending
        assertEquals(10L, consumerInfo.getPending(), "Found entries pending");

        // Now Ack the messages
        collector.getEmittedTuples().stream()
            .map(EmittedTuple::getMessageId)
            .forEach(spout::ack);

        // Small delay waiting for processing.
        Thread.sleep(1000L);

        // Verify that our message were acked in redis.
        consumerInfo = redisTestHelper.getStreamInfo(streamKey, GROUP_NAME, CONSUMER_ID);
        assertNotNull(consumerInfo, "Failed to find consumer info!");

        // Verify we have nothing pending
        assertEquals(0L, consumerInfo.getPending(), "Found entries pending?");

        // Deactivate and close
        spout.deactivate();
        spout.close();

        // Verify mocks
        verify(mockTopologyContext, times(1)).getThisTaskIndex();
    }
}