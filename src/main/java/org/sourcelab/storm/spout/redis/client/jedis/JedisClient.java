package org.sourcelab.storm.spout.redis.client.jedis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sourcelab.storm.spout.redis.Message;
import org.sourcelab.storm.spout.redis.RedisStreamSpoutConfig;
import org.sourcelab.storm.spout.redis.client.Client;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Redis Stream Consumer using the Jedi java library.
 */
public class JedisClient implements Client {
    private static final Logger logger = LoggerFactory.getLogger(JedisClient.class);

    /**
     * The underlying Redis Client.
     */
    private final JedisAdapter adapter;

    /**
     * State for consuming first from consumer's personal pending list,
     * then switching to reading from consumer group messages.
     */
    private boolean hasFinishedPpl = false;

    /**
     * Constructor.
     * @param config Configuration.
     * @param instanceId Which instance number is this running under.
     */
    public JedisClient(final RedisStreamSpoutConfig config, final int instanceId) {
        this(
            // Determine which adapter to use based on what type of redis instance we are
            // communicating with.
            JedisClient.createAdapter(config, instanceId)
        );
    }

    private static JedisAdapter createAdapter(final RedisStreamSpoutConfig config, final int instanceId) {
        final String connectStr = config.getConnectString().replaceAll("redis://", "");
        if (config.isConnectingToCluster()) {
            logger.info("Connecting to RedisCluster at {}", config.getConnectStringMasked());
            return new JedisClusterAdapter(new JedisCluster(HostAndPort.parseString(connectStr)), config, instanceId);
        } else {
            logger.info("Connecting to RedisCluster at {}", config.getConnectStringMasked());
            return new JedisRedisAdapter(new Jedis(HostAndPort.parseString(connectStr)), config, instanceId);
        }
    }

    /**
     * Protected constructor for injecting a RedisClient instance, typically for tests.
     * @param adapter JedisAdapter instance.
     */
    JedisClient(final JedisAdapter adapter) {
        this.adapter = Objects.requireNonNull(adapter);
    }

    @Override
    public void connect() {
        // Connect
        adapter.connect();

        // Start consuming from PPL at first entry.
        adapter.advancePplOffset("0-0");
    }

    @Override
    public List<Message> nextMessages() {
        final List<Message> messages = adapter.consume()
            .stream()
            .map(Map.Entry::getValue)
            .flatMap(Collection::stream)
            .map((entry) -> new Message(entry.getID().toString(), entry.getFields()))
            .collect(Collectors.toList());

        // If we haven't finished consuming from PPL, but we received no messages
        if (!hasFinishedPpl) {
            if (messages.isEmpty()) {
                logger.info("Personal Pending List appears empty, switching to consuming from new messages.");

                // Switch to reading from consumer group
                hasFinishedPpl = true;
                adapter.switchToConsumerGroupMessages();

                // Re-attempt consuming
                return nextMessages();
            } else {
                // Advance last index consumed from PPL so we don't continue to replay old messages.
                final String lastId = messages.get(messages.size() - 1).getId();
                adapter.advancePplOffset(lastId);
            }
        }
        return messages;
    }

    @Override
    public void commitMessage(final String msgId) {
        adapter.commit(msgId);
    }

    @Override
    public void disconnect() {
        adapter.close();
    }
}
