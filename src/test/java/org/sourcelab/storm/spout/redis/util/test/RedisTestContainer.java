package org.sourcelab.storm.spout.redis.util.test;

import org.sourcelab.storm.spout.redis.RedisStreamSpoutConfig;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.startupcheck.MinimumDurationRunningStartupCheckStrategy;

import java.time.Duration;

/**
 * Wrapper on top of TestContainer which attempts to hide the differences between running an Integration
 * test against a Redis instance vs a RedisCluster instance.
 */
public class RedisTestContainer extends FixedHostPortGenericContainer<RedisTestContainer> {
    /**
     * The name of the Docker image to run Redis integration tests against.
     */
    public static final String REDIS_DOCKER_CONTAINER_IMAGE = "redis:6.0.5-alpine";

    /**
     * The name of the Docker image to run RedisCluster integration tests against.
     */
    public static final String REDIS_CLUSTER_DOCKER_CONTAINER_IMAGE = "grokzen/redis-cluster:latest";

    /**
     * This gets set to true if we're running the RedisCluster container.
     */
    private final boolean isCluster;

    /**
     * TestHelper instance.
     */
    private RedisTestHelper redisTestHelper = null;

    /**
     * Constructor.
     * @param dockerImageName Name of the image to launch.
     */
    private RedisTestContainer(final String dockerImageName) {
        super(dockerImageName);

        if (getDockerImageName().equalsIgnoreCase(REDIS_DOCKER_CONTAINER_IMAGE)) {
            this.isCluster = false;
        } else {
            this.isCluster = true;
        }
    }

    /**
     * Factory method for a new TestContainer running a Redis server.
     */
    public static RedisTestContainer newRedisContainer() {
        return new RedisTestContainer(REDIS_DOCKER_CONTAINER_IMAGE)
            .withExposedPorts(6379);
    }

    /**
     * Factory method for a new TestContainer running a RedisCluster server.
     */
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
            // TODO need to come up with a better solution to determining if the cluster is online.
            .withStartupCheckStrategy(
                new MinimumDurationRunningStartupCheckStrategy(Duration.ofSeconds(10))
            );
    }

    /**
     * Getter for the RedisTestHelper utility class configured
     * to talk to the Redis instance running in the TestContainer.
     */
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

    /**
     * Get the port Redis instance is listening on.
     */
    public int getPort() {
        if (isCluster) {
            return 7000;
        } else {
            return getFirstMappedPort();
        }
    }

    /**
     * Get the connection URI to talk to the Redis instance running in the Container.
     */
    public String getConnectStr() {
        return "redis://" + getHost() + ":" + getPort();
    }

    /**
     * Utility method to update a RedisStreamSpoutConfig with the appropriate connection
     * details to talk to the Redis instance running in the Container.
     */
    public RedisStreamSpoutConfig.Builder addConnectionDetailsToConfig(final RedisStreamSpoutConfig.Builder builder) {
        if (isCluster) {
            builder.withClusterNode(getHost(), getPort());
        } else {
            builder.withServer(getHost(), getPort());
        }
        return builder;
    }

    /**
     * Override the default stop() method to also shutdown the RedisTestHelper instance.
     */
    @Override
    public void stop() {
        if (redisTestHelper != null) {
            redisTestHelper.close();
            redisTestHelper = null;
        }
        super.stop();
    }
}
