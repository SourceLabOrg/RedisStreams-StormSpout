package org.sourcelab.storm.spout.redis.util.test;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sourcelab.storm.spout.redis.RedisStreamSpoutConfig;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.startupcheck.MinimumDurationRunningStartupCheckStrategy;

import java.time.Duration;

/**
 * Wrapper on top of TestContainer.
 */
public class RedisTestContainer extends FixedHostPortGenericContainer<RedisTestContainer> {
    public static final String REDIS_DOCKER_CONTAINER_IMAGE = "redis:6.0.5-alpine";
    public static final String REDIS_CLUSTER_DOCKER_CONTAINER_IMAGE = "grokzen/redis-cluster:latest";

    private final boolean isCluster;
    private RedisTestHelper redisTestHelper = null;

    public RedisTestContainer(final String dockerImageName) {
        super(dockerImageName);

        if (getDockerImageName().equalsIgnoreCase(REDIS_DOCKER_CONTAINER_IMAGE)) {
            this.isCluster = false;
        } else {
            this.isCluster = true;
        }
    }

    public static RedisTestContainer newRedisContainer() {
        return new RedisTestContainer(REDIS_DOCKER_CONTAINER_IMAGE)
            .withExposedPorts(6379);
    }

    public RedisTestHelper getRedisTestHelper() {
        if (redisTestHelper == null) {
            if (!isCluster) {
                redisTestHelper = RedisTestHelper.createRedisHelper(getConnectStr());
            } else {
                redisTestHelper = RedisTestHelper.createClusterHelper(getConnectStr());
            }
        }
        return redisTestHelper;
    }

    public int getPort() {
        if (isCluster) {
            return 7000;
        } else {
            return getFirstMappedPort();
        }
    }

    public String getConnectStr() {
        return "redis://" + getHost() + ":" + getPort();
    }

    public RedisStreamSpoutConfig.Builder addConnectionDetailsToConfig(final RedisStreamSpoutConfig.Builder builder) {
        if (isCluster) {
            builder.withCluster(getHost(), getPort());
        } else {
            builder.withServer(getHost(), getPort());
        }
        return builder;
    }

    public static RedisTestContainer newRedisClusterContainer() {
        return new RedisTestContainer(REDIS_CLUSTER_DOCKER_CONTAINER_IMAGE)
            // Fixed ports
            .withFixedExposedPort(7000, 7000)
            .withFixedExposedPort(7001, 7001)
            .withFixedExposedPort(7002, 7002)
            .withFixedExposedPort(7003, 7003)
            .withFixedExposedPort(7004, 7004)
            .withFixedExposedPort(7005, 7005)
            // Override IP so Discovery Works.
            .withEnv("IP", "127.0.0.1")
            .withStartupCheckStrategy(
                new MinimumDurationRunningStartupCheckStrategy(Duration.ofSeconds(10))
            );
    }

    @Override
    public void stop() {
        if (redisTestHelper != null) {
            redisTestHelper.close();
            redisTestHelper = null;
        }
        super.stop();
    }
}
