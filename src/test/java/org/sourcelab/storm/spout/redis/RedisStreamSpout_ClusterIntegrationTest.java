package org.sourcelab.storm.spout.redis;

import org.junit.jupiter.api.Tag;
import org.sourcelab.storm.spout.redis.util.test.RedisTestContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Runs Spout integration tests against a RedisCluster.
 */
@Testcontainers
@Tag("Integration")
public class RedisStreamSpout_ClusterIntegrationTest extends AbstractRedisStreamSpoutIntegrationTest {
    /**
     * This test depends on the following Redis Container.
     */
    @Container
    public RedisTestContainer testContainer = RedisTestContainer.newRedisClusterContainer();

    @Override
    RedisTestContainer getTestContainer() {
        return testContainer;
    }
}
