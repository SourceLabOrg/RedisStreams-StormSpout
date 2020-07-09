package org.sourcelab.storm.spout.redis.util;

import io.lettuce.core.Consumer;
import io.lettuce.core.RedisBusyException;
import io.lettuce.core.RedisClient;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class TestConsumer {
    private static final Logger logger = LoggerFactory.getLogger(TestConsumer.class);

    public static void main(String[] args) {
        final String connectionStr = "redis://localhost:6379";
        final String streamName = "test_stream";
        final String consumerGroupName = "MyGroup";
        final String consumerId = "me_1";

        final RedisClient redisClient = RedisClient.create(connectionStr);
        final StatefulRedisConnection<String, String> connection = redisClient.connect();
        final RedisCommands<String, String> syncCommands = connection.sync();

        try {
            syncCommands.xgroupCreate(
                XReadArgs.StreamOffset.from(streamName, "0-0"),
                consumerGroupName
            );
        }
        catch (RedisBusyException redisBusyException) {
            System.out.println( String.format("\t Group '%s' already exists","application_1"));
        }

        System.out.println("Waiting for new messages");
        while(true) {

            List<StreamMessage<String, String>> messages = syncCommands.xreadgroup(
                Consumer.from(consumerGroupName, consumerId),
                XReadArgs.StreamOffset.lastConsumed(streamName)
            );

            if (!messages.isEmpty()) {
                for (StreamMessage<String, String> message : messages) {
                    System.out.println(message);
                    // Confirm that the message has been processed using XACK
                    syncCommands.xack(streamName, consumerGroupName,  message.getId());
                }
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                break;
            }
        }

        connection.close();
        redisClient.shutdown();
    }
}
