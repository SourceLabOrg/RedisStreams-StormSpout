package org.sourcelab.storm.spout.redis;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

class RedisStreamSpoutConfigTest {

    /**
     * Verifies if you add both Redis servers and RedisCluster nodes it
     * will throw an exception.
     */
    @Test
    void verify_cannotAddBothClusterAndServerEntries() {
        final RedisStreamSpoutConfig.Builder builder = RedisStreamSpoutConfig.newBuilder()
            .withServer("hostname", 123);

        assertThrows(IllegalStateException.class, () -> builder.withClusterNode("clusterhost", 323));
    }

    /**
     * Verifies if you add both Redis servers and RedisCluster nodes it
     * will throw an exception.
     */
    @Test
    void verify_cannotAddBothServerEntriesAndClusterNodes() {
        final RedisStreamSpoutConfig.Builder builder = RedisStreamSpoutConfig.newBuilder()
            .withClusterNode("clusterhost", 323);
        assertThrows(IllegalStateException.class, () -> builder.withServer("host", 123));
    }
}