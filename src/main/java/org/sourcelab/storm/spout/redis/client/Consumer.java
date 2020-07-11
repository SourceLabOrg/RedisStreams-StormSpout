package org.sourcelab.storm.spout.redis.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sourcelab.storm.spout.redis.Configuration;
import org.sourcelab.storm.spout.redis.Message;
import org.sourcelab.storm.spout.redis.funnel.ConsumerFunnel;

import java.util.List;
import java.util.Objects;

/**
 * Background Processing Thread handling Consuming from a Redis Stream Client.
 */
public class Consumer implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

    /**
     * Configuration properties for the client.
     */
    private final Configuration config;

    /**
     * The underlying Redis Client.
     */
    private final Client redisClient;

    /**
     * For thread safe communication between this client thread and the spout thread.
     */
    private final ConsumerFunnel funnel;

    /**
     * Protected constructor for injecting a RedisClient instance, typically for tests.
     * @param redisClient RedisClient instance.
     * @param funnel Funnel instance.
     */
    public Consumer(final Configuration config, final Client redisClient, final ConsumerFunnel funnel) {
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
        redisClient.connect();

        // flip running flag.
        funnel.setIsRunning(true);

        logger.info("Starting to consume new messages from {}", config.getStreamKey());
        while (!funnel.shouldStop()) {
            final List<Message> messages = redisClient.nextMessages();

            // Loop over each message
            messages
                // Push into the funnel.
                // This operation can block if the queue is full.
                .forEach(funnel::addMessage);

            // process acks
            String msgId = funnel.nextAck();
            while (msgId != null) {
                // Confirm that the message has been processed using XACK
                redisClient.commitMessage(msgId);

                // Grab next msg to ack.
                msgId = funnel.nextAck();
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
        logger.info("Spout Requested Shutdown...");

        // Close our connection and shutdown.
        redisClient.disconnect();

        // Flip running flag to false to signal to spout.
        funnel.setIsRunning(false);
    }
}
