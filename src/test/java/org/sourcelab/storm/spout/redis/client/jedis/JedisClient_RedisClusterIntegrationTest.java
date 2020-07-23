package org.sourcelab.storm.spout.redis.client.jedis;

import org.junit.jupiter.api.Tag;
import org.sourcelab.storm.spout.redis.RedisStreamSpoutConfig;
import org.sourcelab.storm.spout.redis.client.AbstractClientIntegrationTest;
import org.sourcelab.storm.spout.redis.client.Client;
import org.sourcelab.storm.spout.redis.util.test.RedisTestContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * NOTE: This Integration test requires Docker to run.
 *
 * This integration test verifies JedisClient against a RedisCluster instance to verify
 * things work as expected when consuming from a RedisCluster.
 *
 * Test cases are defined in {@link AbstractClientIntegrationTest}.
 */
@Testcontainers
@Tag("Integration")
public class JedisClient_RedisClusterIntegrationTest extends AbstractClientIntegrationTest {
    /**
     * This test depends on the following Redis Container.
     */
    @Container
    public RedisTestContainer redisContainer = RedisTestContainer.newRedisContainer();

    @Override
    public RedisTestContainer getTestContainer() {
        return redisContainer;
    }

    @Override
    public Client createClient(final RedisStreamSpoutConfig config, final int instanceId) {
        return new JedisClient(config, instanceId);
    }
}
