package org.sourcelab.storm.spout.redis.client;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sourcelab.storm.spout.redis.Configuration;
import org.sourcelab.storm.spout.redis.Message;
import org.sourcelab.storm.spout.redis.failhandler.NoRetryHandler;
import org.sourcelab.storm.spout.redis.util.TestTupleConverter;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Testcontainers
class LettuceClientIntegrationTest {
    /**
     * This test depends ont he following Redis Container.
     */
    @Container
    public GenericContainer redis = new GenericContainer<>("redis:5.0.3-alpine")
        .withExposedPorts(6379);

    private Configuration config;
    private LettuceClient client;
    private String streamKey;

    @BeforeEach
    public void setUp() {
        // Generate a random stream key
        streamKey = "MyStreamKey" + System.currentTimeMillis();

        // Now we have an address and port for Redis, no matter where it is running
        config = Configuration.newBuilder()
            .withHost(redis.getHost())
            .withPort(redis.getFirstMappedPort())
            .withGroupName("GroupName")
            .withStreamKey(streamKey)
            .withConsumerId("ConsumerId")
            .withFailureHandlerClass(NoRetryHandler.class)
            .withTupleConverterClass(TestTupleConverter.class)
            .build();

        // Create client instance under test.
        client = new LettuceClient(config);

        // Ensure that the key exists!
        createStreamKey(streamKey);
    }

    /**
     * Simple connect and disconnect test.
     */
    @Test
    void testConnectAndDisconnect() {
        client.connect();
        client.disconnect();
    }

    /**
     * Simple connect and disconnect test.
     */
    @Test
    void testConsumeAndCommit() {
        // Connect
        client.connect();

        // Ask for messages.
        List<Message> messages = client.nextMessages();
        assertNotNull(messages, "Should be non-null");
        assertEquals(1, messages.size(), "Should have a single entry");

        // Commit the message.
        messages.forEach((msg) -> client.commitMessage(msg.getId()));

        // Ask for messages, should be empty
        messages = client.nextMessages();
        assertNotNull(messages, "Should be non-null");
        assertEquals(0, messages.size(), "Should be empty");

        // Disconnect
        client.disconnect();
    }

    private void createStreamKey(final String key) {
        final RedisClient redisClient = RedisClient.create(config.getConnectString());
        final StatefulRedisConnection<String, String> connection = redisClient.connect();
        final RedisCommands<String, String> syncCommands = connection.sync();

        final Map<String, String> messageBody = new HashMap<>();
        messageBody.put("key", "0");

        // Write initial value.
        final String messageId = syncCommands.xadd(
            key,
            messageBody
        );

        connection.close();
        redisClient.shutdown();
    }
}