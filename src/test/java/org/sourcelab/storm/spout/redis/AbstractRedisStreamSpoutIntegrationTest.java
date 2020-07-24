package org.sourcelab.storm.spout.redis;

import org.apache.storm.generated.StreamInfo;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsGetter;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.sourcelab.storm.spout.redis.client.ClientType;
import org.sourcelab.storm.spout.redis.example.TestTupleConverter;
import org.sourcelab.storm.spout.redis.failhandler.RetryFailedTuples;
import org.sourcelab.storm.spout.redis.util.outputcollector.EmittedTuple;
import org.sourcelab.storm.spout.redis.util.outputcollector.StubSpoutCollector;
import org.sourcelab.storm.spout.redis.util.test.RedisTestContainer;
import org.sourcelab.storm.spout.redis.util.test.RedisTestHelper;
import org.sourcelab.storm.spout.redis.util.test.StreamConsumerInfo;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Abstract Integration test.  Meant to allow for defining shared test cases that can be validated
 * against both a Redis instance as well as RedisCluster instance.
 */
abstract class AbstractRedisStreamSpoutIntegrationTest {

    /**
     * @return The Appropriate RedisTestContainer instance.
     */
    abstract RedisTestContainer getTestContainer();

    // Configuration values
    private static final String GROUP_NAME = "MyGroupName";
    private static final String CONSUMER_ID_PREFIX = "ConsumerIdPrefix";
    private static final String CONSUMER_ID = CONSUMER_ID_PREFIX + "2";

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
            // Consumer Properties
            .withGroupName(GROUP_NAME)
            .withConsumerIdPrefix(CONSUMER_ID_PREFIX)
            .withStreamKey(streamKey)
            // Failure Handler
            .withNoRetryFailureHandler()
            // Tuple Handler Class
            .withTupleConverter(new TestTupleConverter("timestamp", "value"));

        // Set Connection Properties
        getTestContainer().addConnectionDetailsToConfig(configBuilder);

        // Setup mock
        mockTopologyContext = mock(TopologyContext.class);
        when(mockTopologyContext.getThisTaskIndex())
            .thenReturn(2);

        // Create test helper
        redisTestHelper = getTestContainer().getRedisTestHelper();
    }

    @AfterEach
    void cleanup() {
        // Verify standard metric interactions
        verify(mockTopologyContext, times(3))
            .registerGauge(anyString(), any());

        // Verify all mock interactions accounted for
        verifyNoMoreInteractions(mockTopologyContext);
    }

    /**
     * Most basic lifecycle smoke test.
     */
    @ParameterizedTest
    @EnumSource(ClientType.class)
    void smokeTest_openAndClose(final ClientType clientType) {
        // Inject client type into config
        configBuilder.withClientType(clientType);

        // Create spout
        try (final RedisStreamSpout spout = new RedisStreamSpout(configBuilder.build())) {

            final StubSpoutCollector collector = new StubSpoutCollector();

            // Open spout
            spout.open(stormConfig, mockTopologyContext, new SpoutOutputCollector(collector));

            // Close spout via autocloseable
        }

        // Verify mocks
        verify(mockTopologyContext, times(1)).getThisTaskIndex();
    }

    /**
     * Basic lifecycle smoke test.
     * Basically validating that nothing explodes.
     */
    @ParameterizedTest
    @EnumSource(ClientType.class)
    void smokeTest_openActivateDeactivateAndClose(final ClientType clientType) throws InterruptedException {
        // Inject client type into config
        configBuilder.withClientType(clientType);

        // Create spout
        try (final RedisStreamSpout spout = new RedisStreamSpout(configBuilder.build())) {
            final StubSpoutCollector collector = new StubSpoutCollector();

            // Open spout
            spout.open(stormConfig, mockTopologyContext, new SpoutOutputCollector(collector));

            // activate spout
            spout.activate();

            // Small sleep
            Thread.sleep(3000L);

            // Deactivate and close via Autocloseable
            spout.deactivate();
        }

        // Verify mocks
        verify(mockTopologyContext, times(1)).getThisTaskIndex();
    }

    /**
     * Verifies the behavior when you attempt to connect to a redis instance
     * that does not exist.  Looks like nothing. You get errors in the logs.
     *
     * Disabled for now.
     */
//    @ParameterizedTest
//    @EnumSource(ClientType.class)
    void smokeTest_configureInvalidRedisHost(final ClientType clientType) throws InterruptedException {
        // Inject client type into config
        configBuilder.withClientType(clientType);

        // Lets override the redis host with something invalid
        configBuilder
            .withServer(getTestContainer().getHost(), 124);

        // Create spout
        try (final RedisStreamSpout spout = new RedisStreamSpout(configBuilder.build())) {
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

            // Deactivate and close via Autocloseable
            spout.deactivate();
        }

        // Verify mocks
        verify(mockTopologyContext, times(1)).getThisTaskIndex();
    }

    /**
     * Basic usage test.
     */
    @ParameterizedTest
    @EnumSource(ClientType.class)
    void smokeTest_consumeAndAckMessages(final ClientType clientType) throws InterruptedException {
        // Inject client type into config
        configBuilder.withClientType(clientType);

        // Create spout
        try (final RedisStreamSpout spout = new RedisStreamSpout(configBuilder.build())) {
            final StubSpoutCollector collector = new StubSpoutCollector();

            // Open spout
            spout.open(stormConfig, mockTopologyContext, new SpoutOutputCollector(collector));

            // activate spout
            spout.activate();

            // Lets publish 10 messages to the stream
            final List<String> producedMsgIds = redisTestHelper.produceMessages(streamKey, 10);

            // Now lets try to get those from the spout.
            // Call spout.nextTuple() until the spout has emitted 10 tuples.
            await()
                .atMost(Duration.ofSeconds(10))
                .until(() -> {
                    spout.nextTuple();
                    return collector.getEmittedTuples().size() == 10;
                });

            // Call next tuple a few more times, should be a no-op nothing further should be emitted.
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
                final String expectedValue = "value" + index;
                boolean foundValue = emittedTuple.getTuple().stream()
                    .anyMatch((entry) -> entry.equals(expectedValue));
                assertTrue(foundValue, "Failed to find key tuple value");

                final String expectedMsgIdValue = producedMsgIds.get(index);
                foundValue = emittedTuple.getTuple().stream()
                    .anyMatch((entry) -> entry.equals(expectedMsgIdValue));
                assertTrue(foundValue, "Failed to find msgId tuple value");
            }

            // See that we have 10 items pending in the stream's consumer list.
            await()
                .atMost(Duration.ofSeconds(10))
                .until(() -> {
                    final StreamConsumerInfo consumerInfo = redisTestHelper.getConsumerInfo(streamKey, GROUP_NAME, CONSUMER_ID);
                    assertNotNull(consumerInfo, "Failed to find consumer info!");

                    // Verify we have 10 items pending
                    return consumerInfo.getPending() == 10;
                });

            // Now Ack the messages
            collector.getEmittedTuples().stream()
                .map(EmittedTuple::getMessageId)
                .forEach(spout::ack);

            // We should see the number of pending messages for our consumer drop to 0
            await()
                .atMost(Duration.ofSeconds(10))
                .until(() -> {
                    // Verify that our message were acked in redis.
                    final StreamConsumerInfo consumerInfo = redisTestHelper.getConsumerInfo(streamKey, GROUP_NAME, CONSUMER_ID);
                    assertNotNull(consumerInfo, "Failed to find consumer info!");

                    // Verify we have nothing pending
                    return consumerInfo.getPending() == 0;
                });

            // Deactivate and close via Autocloseable
            spout.deactivate();
        }

        // Verify mocks
        verify(mockTopologyContext, times(1)).getThisTaskIndex();
    }

    /**
     * Basic usage with retry failure handler.
     */
    @ParameterizedTest
    @EnumSource(ClientType.class)
    void smokeTest_consumeFailAndAckMessages(final ClientType clientType) throws InterruptedException {
        // Inject client type into config
        configBuilder.withClientType(clientType);

        // Swap out failure handler, each tuple should be retried a maximum of twice.
        configBuilder.withFailureHandler(new RetryFailedTuples(2));

        // Create spout
        try (final RedisStreamSpout spout = new RedisStreamSpout(configBuilder.build())) {
            final StubSpoutCollector collector = new StubSpoutCollector();

            // Open spout
            spout.open(stormConfig, mockTopologyContext, new SpoutOutputCollector(collector));

            // activate spout
            spout.activate();

            // Lets publish 10 messages to the stream
            List<String> producedMsgIds = redisTestHelper.produceMessages(streamKey, 10);

            // Now lets try to get 5 of those those from the spout...
            // Call spout.nextTuple() until we have at least 5 tuples emitted to the output collector.
            await()
                .atMost(Duration.ofSeconds(10))
                .until(() -> {
                    spout.nextTuple();
                    return collector.getEmittedTuples().size() == 5;
                });

            // Verify what got emitted.
            assertEquals(5, collector.getEmittedTuples().size(), "Should have found 5 emitted tuples.");
            final String expectedStreamId = Utils.DEFAULT_STREAM_ID;
            for (int index = 0; index < 5; index++) {
                final EmittedTuple emittedTuple = collector.getEmittedTuples().get(index);

                // Verify message Id.
                assertEquals(producedMsgIds.get(index), emittedTuple.getMessageId());

                // Verify Stream Id
                assertEquals(expectedStreamId, emittedTuple.getStreamId());

                // Verify tuple value
                assertEquals(3, emittedTuple.getTuple().size(), "Should have 3 values");

                // Look for value
                final String expectedValue = "value" + index;
                boolean foundValue = emittedTuple.getTuple().stream()
                    .anyMatch((entry) -> entry.equals(expectedValue));
                assertTrue(foundValue, "Failed to find key tuple value");

                final String expectedMsgIdValue = producedMsgIds.get(index);
                foundValue = emittedTuple.getTuple().stream()
                    .anyMatch((entry) -> entry.equals(expectedMsgIdValue));
                assertTrue(foundValue, "Failed to find msgId tuple value");
            }

            // Since we have NOT acked any tuples, we should have at least 5 tuples pending with a max of 10
            // depending on how many have been consumed by the spout's consuming thread.
            StreamConsumerInfo consumerInfo = redisTestHelper.getConsumerInfo(streamKey, GROUP_NAME, CONSUMER_ID);
            assertNotNull(consumerInfo, "Failed to find consumer info!");
            assertTrue(consumerInfo.getPending() >= 5, "At least 5 entries pending");
            assertTrue(consumerInfo.getPending() <= 10, "No more than 10 entries pending");

            // We want to setup the following scenario using the 5 tuples the spout has emitted so far.
            // ack the first 3 messages
            // fail the last 2 messages.
            final List<String> messageIdsToFail = new ArrayList<>();
            for (int index = 0; index < 5; index++) {
                // Now ack the first 3 messages
                if (index < 3) {
                    spout.ack(
                        collector.getEmittedTuples().get(index).getMessageId()
                    );
                } else {
                    // Fail the remaining two
                    messageIdsToFail.add((String) collector.getEmittedTuples().get(index).getMessageId());
                    spout.fail(
                        collector.getEmittedTuples().get(index).getMessageId()
                    );
                }
            }

            // And reset our collector
            collector.reset();

            // Small delay waiting for processing.
            // Wait until we have at least 7 pending, which is our 10 total messages, minus the 3 acked ones.
            await()
                .atMost(Duration.ofSeconds(10))
                .until(() -> {
                    final StreamConsumerInfo consumerState = redisTestHelper.getConsumerInfo(streamKey, GROUP_NAME, CONSUMER_ID);

                    // Verify that our message were acked in redis.
                    assertNotNull(consumerState, "Failed to find consumer info!");

                    return consumerState.getPending() == 7;
                });

            // Ask the spout for the next two tuples,
            // The expectation here is that we should get our failed tuples back out.
            await()
                .atMost(Duration.ofSeconds(10))
                .until(() -> {
                    spout.nextTuple();
                    return collector.getEmittedTuples().size() == 2;
                });

            // We should have emitted two tuples, and they should have been our failed tuples.
            assertEquals(2, collector.getEmittedTuples().size());
            assertEquals(messageIdsToFail.get(0), collector.getEmittedTuples().get(0).getMessageId());
            assertEquals(messageIdsToFail.get(1), collector.getEmittedTuples().get(1).getMessageId());

            // Ack the previously failed tuples.
            spout.ack(messageIdsToFail.get(0));
            spout.ack(messageIdsToFail.get(1));

            // Small delay waiting for processing.
            // We're looking for the number of pending to drop to 5, since we've now acked 5 of the 10 original tuples.
            await()
                .atMost(Duration.ofSeconds(10))
                .until(() -> {
                    // Verify that our message were acked in redis.
                    final StreamConsumerInfo consumerState = redisTestHelper.getConsumerInfo(streamKey, GROUP_NAME, CONSUMER_ID);
                    assertNotNull(consumerState, "Failed to find consumer info!");

                    // Verify we have 5 pending
                    return consumerState.getPending() == 5;
                });

            // Deactivate and close via Autocloseable
            spout.deactivate();
        }

        // Verify mocks
        verify(mockTopologyContext, times(1)).getThisTaskIndex();
    }

    /**
     * Verify declareOutputFields using TestTupleConverter.
     */
    @ParameterizedTest
    @EnumSource(ClientType.class)
    void test_declareOutputFields(final ClientType clientType) {
        // Inject client type into config
        configBuilder.withClientType(clientType);

        // Create a test implementation
        final TupleConverter converter = new DummyTupleConverter() ;

        // Update config
        configBuilder.withTupleConverter(converter);

        // Create spout
        try (final RedisStreamSpout spout = new RedisStreamSpout(configBuilder.build())) {
            final StubSpoutCollector collector = new StubSpoutCollector();

            // Open spout and activate.
            spout.open(stormConfig, mockTopologyContext, new SpoutOutputCollector(collector));
            spout.activate();

            // Publish 9 records to redis.
            redisTestHelper.produceMessages(streamKey, 9);

            // Pull via spout
            do {
                spout.nextTuple();
            } while (collector.getEmittedTuples().size() < 9);

            // We should have emitted 9 tuples.
            assertEquals(9, collector.getEmittedTuples().size());

            // Make sure each tuple went out on the correct stream
            for (int index = 0; index < 9; index++) {
                final String expectedStream = "stream" + ((index % 3) + 1);
                final EmittedTuple emittedTuple = collector.getEmittedTuples().get(index);

                // Verify stream
                assertEquals(expectedStream, emittedTuple.getStreamId());
            }

            // Deactivate and close via Autocloseable
            spout.deactivate();
        }

        // Verify mocks
        verify(mockTopologyContext, times(1)).getThisTaskIndex();
    }

    /**
     * Verify spout emits tuples down the correct stream.
     */
    @ParameterizedTest
    @EnumSource(ClientType.class)
    void test_EmitDownSeparateStreams(final ClientType clientType) {
        // Inject client type into config
        configBuilder.withClientType(clientType);

        // Create a test implementation
        final TupleConverter converter = new DummyTupleConverter() ;

        // Update config
        configBuilder.withTupleConverter(converter);

        // Create spout
        try (final RedisStreamSpout spout = new RedisStreamSpout(configBuilder.build())) {
            final StubSpoutCollector collector = new StubSpoutCollector();

            // Open spout
            spout.open(stormConfig, mockTopologyContext, new SpoutOutputCollector(collector));

            // Ask for stream names
            final OutputFieldsGetter getter = new OutputFieldsGetter();
            spout.declareOutputFields(getter);

            // Validate
            final Map<String, StreamInfo> entries = getter.getFieldsDeclaration();
            assertEquals(3, entries.size(), "Should have 3 entries");

            // Verify Stream1
            assertTrue(entries.containsKey("stream1"), "should have entry for 'stream1'");
            StreamInfo info = entries.get("stream1");
            assertEquals(3, info.get_output_fields().size(), "Should have 3 fields");
            assertEquals("field_a", info.get_output_fields().get(0));
            assertEquals("field_b", info.get_output_fields().get(1));
            assertEquals("field_c", info.get_output_fields().get(2));

            // Verify Stream2
            assertTrue(entries.containsKey("stream2"), "should have entry for 'stream2'");
            info = entries.get("stream2");
            assertEquals(3, info.get_output_fields().size(), "Should have 3 fields");
            assertEquals("field_d", info.get_output_fields().get(0));
            assertEquals("field_e", info.get_output_fields().get(1));
            assertEquals("field_f", info.get_output_fields().get(2));

            // Verify Stream3
            assertTrue(entries.containsKey("stream3"), "should have entry for 'stream3'");
            info = entries.get("stream3");
            assertEquals(3, info.get_output_fields().size(), "Should have 3 fields");
            assertEquals("field_g", info.get_output_fields().get(0));
            assertEquals("field_h", info.get_output_fields().get(1));
            assertEquals("field_i", info.get_output_fields().get(2));

            // Deactivate and close via Autocloseable
            spout.deactivate();
        }

        // Verify mocks
        verify(mockTopologyContext, times(1)).getThisTaskIndex();
    }

    /**
     * Verify if tuple converter instance returns null, then the message
     * is simply acked and nothing is emitted.
     */
    @ParameterizedTest
    @EnumSource(ClientType.class)
    void test_NullConversionJustGetsAckedNothingEmitted(final ClientType clientType) {
        // Inject client type into config
        configBuilder.withClientType(clientType);

        // Create a test implementation
        final TupleConverter converter = new NullTupleConverter() ;

        // Update config
        configBuilder.withTupleConverter(converter);

        // Create spout
        try (final RedisStreamSpout spout = new RedisStreamSpout(configBuilder.build())) {
            final StubSpoutCollector collector = new StubSpoutCollector();

            // Open spout and activate
            spout.open(stormConfig, mockTopologyContext, new SpoutOutputCollector(collector));
            spout.activate();

            // Publish 10 records to redis.
            redisTestHelper.produceMessages(streamKey, 10);

            // Attempt to pull via spout.
            // We expect to get nothing.
            final long endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);
            do {
                spout.nextTuple();
            } while (System.currentTimeMillis() < endTime);

            // We should have emitted 0 tuples.
            assertEquals(0, collector.getEmittedTuples().size());

            // Verify that all are showing as acked in redis.
            // See that we have 10 items pending
            StreamConsumerInfo consumerInfo = redisTestHelper.getConsumerInfo(streamKey, GROUP_NAME, CONSUMER_ID);
            assertNotNull(consumerInfo, "Failed to find consumer info!");

            // Verify we have 0 items pending
            assertEquals(0L, consumerInfo.getPending(), "Found entries pending");

            // Deactivate and close via Autocloseable
            spout.deactivate();
        }

        // Verify mocks
        verify(mockTopologyContext, times(1)).getThisTaskIndex();
    }

    /**
     * Dummy Implementation for tests.
     */
    private static class DummyTupleConverter implements TupleConverter {
        private final String[] streams = new String[]{"stream1", "stream2", "stream3"};

        private int counter = 0;

        @Override
        public TupleValue createTuple(final Message message) {
            final String streamName;
            switch (counter) {
                case 0:
                    streamName = "stream1";
                    break;
                case 1:
                    streamName = "stream2";
                    break;
                default:
                    streamName = "stream3";
                    break;
            }
            // Increment counter
            counter = (counter + 1) % 3;

            final List<Object> values = new ArrayList<>();
            values.add("value1");
            values.add("value2");
            values.add("value3");

            return new TupleValue(values, streamName);
        }

        @Override
        public Fields getFieldsFor(final String stream) {
            if ("stream1".equals(stream)) {
                return new Fields("field_a", "field_b", "field_c");
            } else if ("stream2".equals(stream)) {
                return new Fields("field_d", "field_e", "field_f");
            } else if ("stream3".equals(stream)) {
                return new Fields("field_g", "field_h", "field_i");
            }
            throw new IllegalArgumentException("Unknow stream " + stream);
        }

        @Override
        public List<String> streams() {
            return Arrays.asList(streams);
        }
    }

    /**
     * Implementation that always returns null.
     */
    private static class NullTupleConverter implements TupleConverter {

        @Override
        public TupleValue createTuple(final Message message) {
            return null;
        }

        @Override
        public Fields getFieldsFor(final String stream) {
            return new Fields("value");
        }
    }
}
