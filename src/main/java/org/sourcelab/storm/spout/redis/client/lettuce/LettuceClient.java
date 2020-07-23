package org.sourcelab.storm.spout.redis.client.lettuce;

import io.lettuce.core.Consumer;
import io.lettuce.core.RedisBusyException;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.cluster.RedisClusterClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sourcelab.storm.spout.redis.Message;
import org.sourcelab.storm.spout.redis.RedisStreamSpoutConfig;
import org.sourcelab.storm.spout.redis.client.Client;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Redis Stream Consumer using the Lettuce RedisLabs java library.
 */
public class LettuceClient implements Client {
    private static final Logger logger = LoggerFactory.getLogger(LettuceClient.class);

    /**
     * Configuration properties for the client.
     */
    private final RedisStreamSpoutConfig config;

    /**
     * Generated from config.getConsumerIdPrefix() along with the spout's instance
     * id to come to a unique consumerId to support parallelism.
     */
    private final String consumerId;

    /**
     * The underlying Redis Client.
     */
    private final LettuceAdapter adapter;

    /**
     * Re-usable instance to prevent unnecessary garbage creation.
     */
    private final XReadArgs xreadArgs;
    private final Consumer<String> consumerFrom;

    /**
     * State for consuming first from consumer's personal pending list,
     * then switching to reading from consumer group messages.
     */
    private boolean hasFinishedPpl = false;
    private XReadArgs.StreamOffset<String> lastConsumed;

    /**
     * Constructor.
     * @param config Configuration.
     * @param instanceId Which instance number is this running under.
     */
    public LettuceClient(final RedisStreamSpoutConfig config, final int instanceId) {
        this(
            config,
            instanceId,
            // Determine which adapter to use based on what type of redis instance we are communicating with.
            createAdapter(config)
        );
    }

    /**
     * Protected constructor for injecting a RedisClient instance, typically for tests.
     * @param config Configuration.
     * @param instanceId Which instance number is this running under.
     * @param adapter RedisClient instance.
     */
    LettuceClient(final RedisStreamSpoutConfig config, final int instanceId, final LettuceAdapter adapter) {
        this.config = Objects.requireNonNull(config);
        this.adapter = Objects.requireNonNull(adapter);

        // Calculate consumerId
        this.consumerId = config.getConsumerIdPrefix() + instanceId;

        // Create re-usable xReadArgs object.
        xreadArgs = XReadArgs.Builder.noack()
            // Define limit on number of messages to read per request
            .count(config.getMaxConsumePerRead())
            // Require Acks
            .noack(false);

        // Create re-usable ConsumerFrom instance.
        consumerFrom = Consumer.from(config.getGroupName(), consumerId);
    }

    @Override
    public void connect() {
        if (adapter.isConnected()) {
            throw new IllegalStateException("Cannot call connect more than once!");
        }

        adapter.connect();

        // Create re-usable lastConsumed instance, default to consuming from PPL list
        lastConsumed = XReadArgs.StreamOffset.from(config.getStreamKey(), "0-0");
        hasFinishedPpl = false;

        try {
            // Attempt to create consumer group
            adapter.getSyncCommands().xgroupCreate(
                // Start the group at first offset for our key.
                XReadArgs.StreamOffset.from(config.getStreamKey(), "0-0"),
                // Define the group name
                config.getGroupName(),
                // Create the stream if it doesn't already exist.
                XGroupCreateArgs.Builder
                    .mkstream(true)
            );
        }
        catch (final RedisBusyException redisBusyException) {
            // Consumer group already exists, that's ok. Just swallow this.
            logger.debug("Group {} for key {} already exists.", config.getGroupName(), config.getStreamKey());
        }
        catch (final RedisCommandExecutionException exception) {
            logger.error(
                "Key {} does not exist or is invalid! {}",
                config.getStreamKey(), exception.getMessage(), exception
            );

            // Re-throw exception
            throw exception;
        }
    }

    @Override
    public List<Message> nextMessages() {
        // Get next batch of messages.
        final List<StreamMessage<String, String>> entries = adapter.getSyncCommands().xreadgroup(
            consumerFrom,
            xreadArgs,
            lastConsumed
        );

        // Loop over each message
        final List<Message> messages = entries.stream()
            // Map into Message Object
            .map((streamMsg) -> new Message(streamMsg.getId(), streamMsg.getBody()))
            .collect(Collectors.toList());

        if (!hasFinishedPpl) {
            if (messages.isEmpty()) {
                logger.info("Personal Pending List appears empty, switching to consuming from new messages.");

                hasFinishedPpl = true;
                lastConsumed = XReadArgs.StreamOffset.lastConsumed(config.getStreamKey());

                // Re-attempt consuming
                return nextMessages();
            } else {
                // Advance last index consumed from PPL so we don't continue to replay old messages.
                final String lastId = messages.get(messages.size() - 1).getId();
                lastConsumed = XReadArgs.StreamOffset.from(config.getStreamKey(), lastId);
            }
        }

        return messages;
    }

    @Override
    public void commitMessage(final String msgId) {
        // Confirm that the message has been processed using XACK
        adapter.getSyncCommands().xack(
            config.getStreamKey(),
            config.getGroupName(),
            msgId
        );
    }

    @Override
    public void disconnect() {
        adapter.shutdown();
    }

    /**
     * Factory method for creating the appropriate adapter based on configuration.
     * @param config Spout configuration.
     * @return Appropriate Adapter.
     */
    private static LettuceAdapter createAdapter(final RedisStreamSpoutConfig config) {
        if (config.isConnectingToCluster()) {
            logger.info("Connecting to RedisCluster at {}", config.getConnectStringMasked());
            return new LettuceClusterAdapter(RedisClusterClient.create(config.getConnectString()));
        } else {
            logger.info("Connecting to Redis server at {}", config.getConnectStringMasked());
            return new LettuceRedisAdapter(RedisClient.create(config.getConnectString()));
        }
    }
}
