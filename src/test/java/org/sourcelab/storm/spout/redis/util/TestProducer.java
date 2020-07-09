package org.sourcelab.storm.spout.redis.util;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class TestProducer {
    private static final Logger logger = LoggerFactory.getLogger(TestProducer.class);

    public static void main(String[] args) {
        final String connectionStr = "redis://localhost:6379";
        final String streamName = "test_stream";
        final int numRecords = 100;

        final RedisClient redisClient = RedisClient.create(connectionStr);
        final StatefulRedisConnection<String, String> connection = redisClient.connect();
        final RedisCommands<String, String> syncCommands = connection.sync();

        for (int index = 0; index < numRecords; index++) {
            final Map<String, String> messageBody = new HashMap<>();
            messageBody.put( "key", String.valueOf(index) );
            messageBody.put( "timestamp", String.valueOf(System.currentTimeMillis()) );

            final String messageId = syncCommands.xadd(
                streamName,
                messageBody
            );

            logger.info("Wrote key {}", messageId);
        }

        connection.close();
        redisClient.shutdown();
    }
}
