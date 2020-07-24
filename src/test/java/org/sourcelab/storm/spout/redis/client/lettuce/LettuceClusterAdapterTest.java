package org.sourcelab.storm.spout.redis.client.lettuce;

import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
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

class LettuceClusterAdapterTest {
    private RedisClusterClient mockClusterClient;
    private StatefulRedisClusterConnection mockConnection;

    @BeforeEach
    void setup() {
        mockClusterClient = mock(RedisClusterClient.class);
        mockConnection = mock(StatefulRedisClusterConnection.class);
    }

    @AfterEach
    void cleanup() {
        verifyNoMoreInteractions(mockClusterClient);
        verifyNoMoreInteractions(mockConnection);
    }

    @Test
    void testAdapter() {
        final RedisAdvancedClusterCommands<String, String> mockCommands = mock(RedisAdvancedClusterCommands.class);

        // Setup mocks
        when(mockClusterClient.connect())
            .thenReturn(mockConnection);

        when(mockConnection.sync())
            .thenReturn(mockCommands);

        // Create instance
        final LettuceClusterAdapter adapter = new LettuceClusterAdapter(mockClusterClient);

        // Verify "not connected"
        assertFalse(adapter.isConnected(), "Should return false");

        // Call connect.
        adapter.connect();

        // Verify "connected"
        assertTrue(adapter.isConnected(), "Should return true");

        // Verify interactions
        verify(mockClusterClient, times(1))
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
        verify(mockClusterClient, times(1)).shutdown();
    }
}