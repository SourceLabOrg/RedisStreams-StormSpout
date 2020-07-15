package org.sourcelab.storm.spout.redis;

import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sourcelab.storm.spout.redis.util.test.RedisTestContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 *
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
