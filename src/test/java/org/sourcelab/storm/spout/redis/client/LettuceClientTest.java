package org.sourcelab.storm.spout.redis.client;

import io.lettuce.core.Consumer;
import io.lettuce.core.RedisClient;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.sourcelab.storm.spout.redis.Configuration;
import org.sourcelab.storm.spout.redis.failhandler.NoRetryHandler;
import org.sourcelab.storm.spout.redis.funnel.MemoryFunnel;
import org.sourcelab.storm.spout.redis.util.TestTupleConverter;

import java.util.Collections;
import java.util.HashMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Tests over LettuceClient using a mock RedisClient instance.
 */
class LettuceClientTest {

    // Create config
    private final Configuration config = Configuration.newBuilder()
        .withHost("host")
        .withPort(123)
        .withGroupName("GroupName")
        .withStreamKey("Key")
        .withConsumerId("ConsumerId")
        .withFailureHandlerClass(NoRetryHandler.class)
        .withTupleConverterClass(TestTupleConverter.class)
        .build();

    // Mocks
    private RedisClient mockRedisClient;
    private StatefulRedisConnection<String, String> mockConnection;
    private RedisCommands<String, String> mockRedisCommands;

    private MemoryFunnel funnel;

    @BeforeEach
    void setup() {
        mockRedisClient = mock(RedisClient.class);
        mockConnection = mock(StatefulRedisConnection.class);
        mockRedisCommands = mock(RedisCommands.class);
        funnel = new MemoryFunnel(config, new HashMap<>());
    }

    @AfterEach
    void cleanup() {
        // Ensure all interactions accounted for.
        verifyNoMoreInteractions(mockRedisClient);
    }

//    @Test
//    void smokeTest() throws InterruptedException {
//        final Consumer expectedConsumer = Consumer.from(config.getGroupName(), config.getConsumerId());
//        final XReadArgs.StreamOffset<String> expectedOffset = XReadArgs.StreamOffset.lastConsumed(config.getStreamKey());
//
//        // Setup our mocks
//        when(mockRedisClient.connect())
//            .thenReturn(mockConnection);
//
//        when(mockConnection.sync())
//            .thenReturn(mockRedisCommands);
//
//        when(mockRedisCommands.xgroupCreate(eq(
//            XReadArgs.StreamOffset.from(config.getStreamKey(), "0-0")), eq(config.getGroupName())
//        )).thenReturn("NotUsed");
//
//        when(mockRedisCommands.xreadgroup(eq(expectedConsumer), eq(expectedOffset)))
//            .thenReturn(Collections.emptyList());
//
//        // Create a mock redis client
//        final LettuceClient client = new LettuceClient(
//            config, mockRedisClient, funnel
//        );
//
//        // Create a new Thread
//        final Thread clientThread = new Thread(client);
//
//        // Start thread.
//        clientThread.start();
//
//        // Wait for startup.
//        while (!funnel.isRunning()) {
//            Thread.sleep(500L);
//        }
//
//        // TODO Finish writing test.
//        Thread.sleep(5000L);
//
//        funnel.requestStop();
//
//        // Wait for the thread to finish.
//        clientThread.join();
//
//        // Verify connect interactions
//        verify(mockRedisClient, Mockito.times(1))
//            .connect();
//        verify(mockConnection, Mockito.times(1))
//            .sync();
//        // TODO capture parameters and verify
//        verify(mockRedisCommands, Mockito.times(1))
//            .xgroupCreate(any(), any());
//
//        // Consuming
//        // TODO capture parameters and verify.
//        verify(mockRedisCommands, Mockito.atLeastOnce())
//            .xreadgroup(any(), any());
//
//        // Verify shutdown interactions
//        verify(mockConnection, Mockito.times(1))
//            .close();
//        verify(mockRedisClient, Mockito.times(1))
//            .shutdown();
//    }

}