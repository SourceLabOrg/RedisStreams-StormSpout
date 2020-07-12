package org.sourcelab.storm.spout.redis.util;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sourcelab.storm.spout.redis.Configuration;
import org.sourcelab.storm.spout.redis.failhandler.NoRetryHandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Test Helper for interacting with a live Redis server.
 */
public class RedisTestHelper implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(RedisTestHelper.class);

    private final Configuration config;
    private final RedisClient redisClient;
    private final StatefulRedisConnection<String, String> connection;

    /**
     * Constructor.
     * @param config Configuration.
     */
    public RedisTestHelper(final Configuration config) {
        this.config = Objects.requireNonNull(config);
        this.redisClient = RedisClient.create(config.getConnectString());
        this.connection = redisClient.connect();
    }

    public void createStreamKey(final String key) {
        Objects.requireNonNull(key);
        final RedisCommands<String, String> syncCommands = connection.sync();

        final Map<String, String> messageBody = new HashMap<>();
        messageBody.put("key", "0");

        // Write initial value.
        final String messageId = syncCommands.xadd(
            key,
            messageBody
        );
    }

    public List<String> produceMessages(final String key, final int numberOfMessages) {
        final List<String> messageIds = new ArrayList<>();

        final RedisCommands<String, String> commands = connection.sync();

        for (int index = 0; index < numberOfMessages; index++) {
            final Map<String, String> messageBody = new HashMap<>();
            messageBody.put( "key", String.valueOf(index) );
            messageBody.put( "timestamp", String.valueOf(System.currentTimeMillis()) );

            final String messageId = commands.xadd(
                key,
                messageBody
            );
            messageIds.add(messageId);
        }

        return messageIds;
    }

    @Override
    public void close() {
        connection.close();
        redisClient.shutdown();
    }
}
