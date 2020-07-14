package org.sourcelab.storm.spout.redis.util.test;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private final RedisClient redisClient;
    private final StatefulRedisConnection<String, String> connection;

    /**
     * Constructor.
     * @param connectStr Redis Connect string.
     */
    public RedisTestHelper(final String connectStr) {
        this.redisClient = RedisClient.create(connectStr);
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

    public List<String> produceMessages(final String stream, final int numberOfMessages) {
        final List<String> messageIds = new ArrayList<>();

        final RedisCommands<String, String> commands = connection.sync();

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
        final RedisCommands<String, String> commands = connection.sync();

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
        final RedisCommands<String, String> commands = connection.sync();
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

    @Override
    public void close() {
        connection.close();
        redisClient.shutdown();
    }
}
