package org.sourcelab.storm.spout.redis.util;

import org.apache.storm.task.TopologyContext;
import org.junit.jupiter.api.Test;
import org.sourcelab.storm.spout.redis.Configuration;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ConfigUtilTest {

    /**
     * Tests loading the config using Strings for number values.
     */
    @Test
    void test_loadFromMap_usingStringsForNumbers() {

        // Define input map
        final Map<String, Object> configMap = new HashMap<>();

        // Set Connection Properties
        final String expectedHost = "localhost";
        final String expectedPortStr = "1234";
        final String expectedPassword = "MyPassword";
        configMap.put(ConfigUtil.REDIS_SERVER_HOST, expectedHost);
        configMap.put(ConfigUtil.REDIS_SERVER_PORT, expectedPortStr);
        configMap.put(ConfigUtil.REDIS_SERVER_PASSWORD, expectedPassword);

        // Set Consumer Properties.
        final String expectedGroupName = "MyGroupName";
        final String expectedConsumerIdPrefix = "ConsumerIdPrefix";
        final String expectedStreamKey = "StreamKey";
        configMap.put(ConfigUtil.REDIS_CONSUMER_GROUP_NAME, expectedGroupName);
        configMap.put(ConfigUtil.REDIS_CONSUMER_STREAM_KEY, expectedStreamKey);
        configMap.put(ConfigUtil.REDIS_CONSUMER_CONSUMER_ID_PREFIX, expectedConsumerIdPrefix);

        // Consumer Properties
        final String expectedDelayMillisStr = "1200";
        final long expectedDelayMillis = 1200L;
        final String expectedAckQueueSizeStr = "1025";
        final int expectedAckQueueSize = 1025;
        final String expectedTupleQueueSizeStr = "1026";
        final int expectedTupleQueueSize = 1026;
        configMap.put(ConfigUtil.CONSUMER_DELAY_MILLIS, expectedDelayMillisStr);
        configMap.put(ConfigUtil.CONSUMER_MAX_ACK_QUEUE_SIZE, expectedAckQueueSizeStr);
        configMap.put(ConfigUtil.CONSUMER_MAX_TUPLE_QUEUE_SIZE, expectedTupleQueueSizeStr);

        // Failure Handler
        final String expectedFailureHandlerMaxRetriesStr = "123";
        final String expectedFailureHandlerClass = "RandomFailureClass";
        configMap.put(ConfigUtil.FAILURE_HANDLER_MAX_RETRIES, expectedFailureHandlerMaxRetriesStr);
        configMap.put(ConfigUtil.FAILURE_HANDLER_CLASS, expectedFailureHandlerClass);

        // Tuple Handler Class
        final String expectedTupleConverterClass = "TupleConverterClass";
        configMap.put(ConfigUtil.TUPLE_CONVERTER_CLASS, expectedTupleConverterClass);

        // Create a mock Topology Context
        final TopologyContext mockTopologyContext = mock(TopologyContext.class);
        when(mockTopologyContext.getThisTaskIndex())
            .thenReturn(2);

        // Build config
        final Configuration config = ConfigUtil.load(configMap, mockTopologyContext);

        // Validate
        assertNotNull(config);

        // Host Properties
        final String expectedConnectStr = "redis://" + expectedPassword + "@" + expectedHost + ":" + expectedPortStr;
        assertEquals(expectedConnectStr, config.getConnectString());

        // Consumer properties
        assertEquals(expectedGroupName, config.getGroupName());
        assertEquals(expectedStreamKey, config.getStreamKey());
        assertEquals(expectedConsumerIdPrefix + "2", config.getConsumerId());

        // Failure Handler
        assertEquals(expectedFailureHandlerClass, config.getFailureHandlerClass());

        // Tuple Converter Handler
        assertEquals(expectedTupleConverterClass, config.getTupleConverterClass());

        // Consumer Properties
        assertEquals(expectedDelayMillis, config.getConsumerDelayMillis());
        assertEquals(expectedAckQueueSize, config.getMaxAckQueueSize());
        assertEquals(expectedTupleQueueSize, config.getMaxTupleQueueSize());

        // Verify mock interactions.
        verify(mockTopologyContext, times(1))
            .getThisTaskIndex();
    }

    /**
     * Tests loading the config using Numbers for number values.
     */
    @Test
    void test_loadFromMap_usingNumbers() {

        // Define input map
        final Map<String, Object> configMap = new HashMap<>();

        // Set Connection Properties
        final String expectedHost = "localhost";
        final int expectedPort = 1234;
        final String expectedPassword = "MyPassword";
        configMap.put(ConfigUtil.REDIS_SERVER_HOST, expectedHost);
        configMap.put(ConfigUtil.REDIS_SERVER_PORT, expectedPort);
        configMap.put(ConfigUtil.REDIS_SERVER_PASSWORD, expectedPassword);

        // Set Consumer Properties.
        final String expectedGroupName = "MyGroupName";
        final String expectedConsumerIdPrefix = "ConsumerIdPrefix";
        final String expectedStreamKey = "StreamKey";
        configMap.put(ConfigUtil.REDIS_CONSUMER_GROUP_NAME, expectedGroupName);
        configMap.put(ConfigUtil.REDIS_CONSUMER_STREAM_KEY, expectedStreamKey);
        configMap.put(ConfigUtil.REDIS_CONSUMER_CONSUMER_ID_PREFIX, expectedConsumerIdPrefix);

        // Consumer Properties
        final long expectedDelayMillis = 1200L;
        final int expectedAckQueueSize = 1025;
        final int expectedTupleQueueSize = 1026;
        configMap.put(ConfigUtil.CONSUMER_DELAY_MILLIS, expectedDelayMillis);
        configMap.put(ConfigUtil.CONSUMER_MAX_ACK_QUEUE_SIZE, expectedAckQueueSize);
        configMap.put(ConfigUtil.CONSUMER_MAX_TUPLE_QUEUE_SIZE, expectedTupleQueueSize);

        // Failure Handler
        final String expectedFailureHandlerMaxRetriesStr = "123";
        final String expectedFailureHandlerClass = "RandomFailureClass";
        configMap.put(ConfigUtil.FAILURE_HANDLER_MAX_RETRIES, expectedFailureHandlerMaxRetriesStr);
        configMap.put(ConfigUtil.FAILURE_HANDLER_CLASS, expectedFailureHandlerClass);

        // Tuple Handler Class
        final String expectedTupleConverterClass = "TupleConverterClass";
        configMap.put(ConfigUtil.TUPLE_CONVERTER_CLASS, expectedTupleConverterClass);

        // Create a mock Topology Context
        final TopologyContext mockTopologyContext = mock(TopologyContext.class);
        when(mockTopologyContext.getThisTaskIndex())
            .thenReturn(2);

        // Build config
        final Configuration config = ConfigUtil.load(configMap, mockTopologyContext);

        // Validate
        assertNotNull(config);

        // Host Properties
        final String expectedConnectStr = "redis://" + expectedPassword + "@" + expectedHost + ":" + expectedPort;
        assertEquals(expectedConnectStr, config.getConnectString());

        // Consumer properties
        assertEquals(expectedGroupName, config.getGroupName());
        assertEquals(expectedStreamKey, config.getStreamKey());
        assertEquals(expectedConsumerIdPrefix + "2", config.getConsumerId());

        // Failure Handler
        assertEquals(expectedFailureHandlerClass, config.getFailureHandlerClass());

        // Tuple Converter Handler
        assertEquals(expectedTupleConverterClass, config.getTupleConverterClass());

        // Consumer Properties
        assertEquals(expectedDelayMillis, config.getConsumerDelayMillis());
        assertEquals(expectedAckQueueSize, config.getMaxAckQueueSize());
        assertEquals(expectedTupleQueueSize, config.getMaxTupleQueueSize());

        // Verify mock interactions.
        verify(mockTopologyContext, times(1))
            .getThisTaskIndex();
    }

}