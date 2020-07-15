package org.sourcelab.storm.spout.redis.client;

import org.junit.jupiter.api.Tag;
import org.sourcelab.storm.spout.redis.util.test.RedisTestContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * NOTE: This Integration test requires Docker to run.
 *
 * This integration test verifies LettuceClient against a RedisCluster instance to verify
 * things work as expected when consuming from a RedisCluster.
 *
 * Test cases are defined in {@link AbstractLettuceClientIntegrationTest}.
 */
@Testcontainers
@Tag("Integration")
public class LettuceClient_RedisClusterIntegrationTest extends AbstractLettuceClientIntegrationTest {
    /**
     * This test depends on the following Redis Container.
     */
    @Container
    public RedisTestContainer redisContainer = RedisTestContainer.newRedisClusterContainer();

    @Override
    RedisTestContainer getTestContainer() {
        return redisContainer;
    }
}
