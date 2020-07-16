package org.sourcelab.storm.spout.redis.client;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class LettuceRedisAdapterTest {

    private RedisClient mockRedisClient;
    private StatefulRedisConnection mockConnection;

    @BeforeEach
    void setup() {
        mockRedisClient = mock(RedisClient.class);
        mockConnection = mock(StatefulRedisConnection.class);
    }

    @AfterEach
    void cleanup() {
        verifyNoMoreInteractions(mockRedisClient);
        verifyNoMoreInteractions(mockConnection);
    }

    @Test
    void testAdapter() {
        final RedisCommands<String, String> mockCommands = mock(RedisCommands.class);

        // Setup mocks
        when(mockRedisClient.connect())
            .thenReturn(mockConnection);

        when(mockConnection.sync())
            .thenReturn(mockCommands);

        // Create instance
        final LettuceRedisAdapter adapter = new LettuceRedisAdapter(mockRedisClient);

        // Verify "not connected"
        assertFalse(adapter.isConnected(), "Should return false");

        // Call connect.
        adapter.connect();

        // Verify "connected"
        assertTrue(adapter.isConnected(), "Should return true");

        // Verify interactions
        verify(mockRedisClient, times(1))
            .connect();

        // Call sync multiple times
        assertNotNull(adapter.getSyncCommands());
        assertNotNull(adapter.getSyncCommands());
        assertNotNull(adapter.getSyncCommands());

        // Only interacts with mock once.
        verify(mockConnection, times(1))
            .sync();

        // Call shutdown
        adapter.shutdown();

        verify(mockConnection, times(1)).close();
        verify(mockRedisClient, times(1)).shutdown();
    }
}