package org.sourcelab.storm.spout.redis.util.test;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.api.sync.RedisStreamCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import org.sourcelab.storm.spout.redis.client.LettuceAdapter;
import org.sourcelab.storm.spout.redis.client.LettuceClusterClient;
import org.sourcelab.storm.spout.redis.client.LettuceRedisClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Test Helper for interacting with a live Redis server.
 */
public class RedisTestHelper implements AutoCloseable {
    private final LettuceAdapter redisClient;

    @Deprecated
    public RedisTestHelper(final String connectStr) {
        redisClient = new LettuceRedisClient(RedisClient.create(connectStr));
        redisClient.connect();
    }

    /**
     * Constructor.
     */
    public RedisTestHelper(final LettuceAdapter adapter) {
        this.redisClient = Objects.requireNonNull(adapter);
        this.redisClient.connect();
    }

    public static RedisTestHelper createRedisHelper(final String connectStr) {
        return new RedisTestHelper(
            new LettuceRedisClient(RedisClient.create(connectStr))
        );
    }

    public static RedisTestHelper createClusterHelper(final String connectStr) {
        return new RedisTestHelper(
            new LettuceClusterClient(RedisClusterClient.create(connectStr))
        );
    }

    public void createStreamKey(final String key) {
        Objects.requireNonNull(key);
        final RedisStreamCommands<String, String> syncCommands = redisClient.getSyncCommands();

        final Map<String, String> messageBody = new HashMap<>();
        messageBody.put("key", "0");

        // Write initial value.
        final String messageId = syncCommands.xadd(
            key,
            messageBody
        );
    }

    public List<String> produceMessages(final String stream, final int numberOfMessages) {
        final List<String> messageIds = new ArrayList<>();

        final RedisStreamCommands<String, String> commands = redisClient.getSyncCommands();

        for (int index = 0; index < numberOfMessages; index++) {
            final Map<String, String> messageBody = new HashMap<>();
            messageBody.put("value", "value" + index);
            messageBody.put("timestamp", String.valueOf(System.currentTimeMillis()));

            final String messageId = commands.xadd(
                stream,
                messageBody
            );
            messageIds.add(messageId);
        }

        return messageIds;
    }

    /**
     * Produce a single message.
     * @param stream Stream key
     * @param values Values to produce.
     * @return messageId produced.
     */
    public String produceMessage(final String stream, final Map<String, String> values) {
        final RedisStreamCommands<String, String> commands = redisClient.getSyncCommands();

        return commands.xadd(
            stream,
            values
        );
    }

    /**
     * Given a stream key, groupName, and consumerId, get details about that consumer.
     * @param streamKey Stream name.
     * @param groupName Group name.
     * @param consumerId Consumer name.
     * @return StreamConsumerInfo representing information about that consumer.
     */
    public StreamConsumerInfo getConsumerInfo(final String streamKey, final String groupName, final String consumerId) {
        return getStreamInfo(streamKey, groupName).get(consumerId);
    }

    /**
     * Given a stream key and group name, get all the consumer details associated.
     * @param streamKey Stream name.
     * @param groupName Group name.
     * @return Map of ConsumerId => Details about that consumer.
     */
    public Map<String, StreamConsumerInfo> getStreamInfo(final String streamKey, final String groupName) {
        final RedisStreamCommands<String, String> commands = redisClient.getSyncCommands();
        final List<Object> result = commands.xinfoConsumers(streamKey, groupName);

        final Map<String, StreamConsumerInfo> consumerInfos = new HashMap<>();

        // Attempt to parse into something actually usable.
        for (final Object entryObj : result) {
            final StreamConsumerInfo.Builder builder = StreamConsumerInfo.newBuilder();

            if (!(entryObj instanceof List)) {
                continue;
            }
            final List<Object> entry = (List) entryObj;
            for (int index = 0; index < entry.size(); index++) {
                if (!(entry.get(index) instanceof String)) {
                    continue;
                }

                // Parse name
                if (entry.get(index).equals("name")) {
                    builder.withConsumerId((String) entry.get(index + 1));
                    index++;
                    continue;
                }

                // Parse Pending
                if (entry.get(index).equals("pending")) {
                    builder.withPending((Long) entry.get(index + 1));
                    index++;
                    continue;
                }

                // Parse Idle
                if (entry.get(index).equals("idle")) {
                    builder.withIdle((Long) entry.get(index + 1));
                    index++;
                    continue;
                }
            }

            consumerInfos.put(builder.build().getConsumerId(), builder.build());
        }

        return consumerInfos;
    }

    public void close() {
        redisClient.shutdown();
    }
}
