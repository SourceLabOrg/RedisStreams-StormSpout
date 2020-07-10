package org.sourcelab.storm.spout.redis.client;

import io.lettuce.core.Consumer;
import io.lettuce.core.RedisBusyException;
import io.lettuce.core.RedisClient;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sourcelab.storm.spout.redis.Configuration;
import org.sourcelab.storm.spout.redis.Message;
import org.sourcelab.storm.spout.redis.funnel.ConsumerFunnel;

import java.util.List;
import java.util.Objects;

/**
 * Redis Stream Consumer using the Lettuce RedisLabs java library.
 */
public class LettuceClient implements Client, Runnable {
    private static final Logger logger = LoggerFactory.getLogger(LettuceClient.class);

    /**
     * Configuration properties for the client.
     */
    private final Configuration config;

    /**
     * The underlying Redis Client.
     */
    private final RedisClient redisClient;

    /**
     * For thread safe communication between this client thread and the spout thread.
     */
    private final ConsumerFunnel funnel;

    /**
     * Constructor.
     * @param config Configuration.
     * @param funnel Funnel instance.
     */
    public LettuceClient(final Configuration config, final ConsumerFunnel funnel) {
        this(
            config,
            RedisClient.create(config.getConnectString()),
            funnel
        );
    }

    /**
     * Protected constructor for injecting a RedisClient instance, typically for tests.
     * @param config Configuration.
     * @param redisClient RedisClient instance.
     * @param funnel Funnel instance.
     */
    LettuceClient(final Configuration config, final RedisClient redisClient, final ConsumerFunnel funnel) {
        this.config = Objects.requireNonNull(config);
        this.redisClient = Objects.requireNonNull(redisClient);
        this.funnel = Objects.requireNonNull(funnel);
    }

    /**
     * Intended to be run by a background processing Thread.
     * This will continue running and not return until the Funnel has notified
     * this thread to stop.
     */
    @Override
    public void run() {
        // Connect
        final StatefulRedisConnection<String, String> connection = redisClient.connect();
        final RedisCommands<String, String> syncCommands = connection.sync();

        // flip running flag.
        funnel.setIsRunning(true);

        try {
            // Attempt to create consumer group
            syncCommands.xgroupCreate(
                XReadArgs.StreamOffset.from(config.getStreamKey(), "0-0"),
                config.getGroupName()
            );
        }
        catch (final RedisBusyException redisBusyException) {
            // Consumer group already exists, that's ok. Just swallow this.
            logger.debug("Group {} already exists.", config.getGroupName());
        }

        logger.info("Starting to consume new messages from {}", config.getStreamKey());
        while (!funnel.shouldStop()) {
            // Get next batch of messages.
            final List<StreamMessage<String, String>> messages = syncCommands.xreadgroup(
                Consumer.from(config.getGroupName(), config.getConsumerId()),
                XReadArgs.StreamOffset.lastConsumed(config.getStreamKey())
            );

            // Loop over each message
            messages.stream()
                // Map into Message Object
                // TODO is streamMsg.getId() universally unique?? Or does it repeat?
                .map((streamMsg) -> new Message(streamMsg.getId(), streamMsg.getBody()))
                // And push into the funnel.
                // This operation can block if the queue is full.
                .forEach(funnel::addMessage);

            // process acks
            String msgId = funnel.getNextAck();
            while (msgId != null) {
                // Confirm that the message has been processed using XACK
                syncCommands.xack(
                    config.getStreamKey(),
                    config.getGroupName(),
                    msgId
                );

                // Grab next msg to ack.
                msgId = funnel.getNextAck();
            }

            // If configured with a delay
            if (config.getConsumerDelayMillis() > 0) {
                // Small delay.
                try {
                    Thread.sleep(config.getConsumerDelayMillis());
                } catch (final InterruptedException exception) {
                    logger.info("Caught interrupt, stopping consumer", exception);
                    break;
                }
            }
        }

        // Close our connection and shutdown.
        connection.close();
        redisClient.shutdown();

        // Flip running flag to false to signal to spout.
        funnel.setIsRunning(false);
    }
}
