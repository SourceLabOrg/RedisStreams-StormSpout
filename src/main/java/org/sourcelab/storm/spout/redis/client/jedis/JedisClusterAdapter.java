package org.sourcelab.storm.spout.redis.client.jedis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sourcelab.storm.spout.redis.RedisStreamSpoutConfig;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.StreamEntry;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 *
 */
public class JedisClusterAdapter implements JedisAdapter {
    private static final Logger logger = LoggerFactory.getLogger(JedisClusterAdapter.class);

    private final JedisCluster jedisCluster;

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
     * Contains the position key to read from.
     */
    private Map.Entry<String, StreamEntryID> streamPositionKey;

    public JedisClusterAdapter(final JedisCluster jedisCluster, final RedisStreamSpoutConfig config, final int instanceId) {
        this.jedisCluster = Objects.requireNonNull(jedisCluster);
        this.config = Objects.requireNonNull(config);
        this.consumerId = config.getConsumerIdPrefix() + instanceId;
    }

    @Override
    public boolean isConnected() {
        return true;
    }

    @Override
    public void connect() {
        // Attempt to create consumer group
        try {
            jedisCluster.xgroupCreate(config.getStreamKey(), config.getGroupName(), new StreamEntryID(), true);
        } catch (final JedisDataException exception) {
            // Consumer group already exists, that's ok. Just swallow this.
            logger.debug(
                "Group {} for key {} already exists? : {}", config.getGroupName(), config.getStreamKey(),
                exception.getMessage(), exception
            );
        }

        // Default to requesting entries from our personal pending queue.
        switchToPpl("0-0");
    }

    @Override
    public List<Map.Entry<String, List<StreamEntry>>> consume() {
        final List<Map.Entry<String, List<StreamEntry>>> entries = jedisCluster.xreadGroup(
            config.getGroupName(),
            consumerId,
            config.getMaxConsumePerRead(),
            2000L,
            false,
            streamPositionKey
        );

        if (entries == null) {
            return Collections.emptyList();
        }
        return entries;
    }

    @Override
    public void commit(final String msgId) {
        jedisCluster.xack(
            config.getStreamKey(),
            config.getGroupName(),
            new StreamEntryID(msgId)
        );
    }

    @Override
    public void close() {
        jedisCluster.close();
    }

    @Override
    public void switchToPpl(final String lastMsgId) {
        streamPositionKey = new AbstractMap.SimpleEntry<>(
            config.getStreamKey(),
            new StreamEntryID(lastMsgId)
        );
    }

    @Override
    public void switchToConsumerGroupMessages() {
        streamPositionKey = new AbstractMap.SimpleEntry<>(
            config.getStreamKey(),
            StreamEntryID.UNRECEIVED_ENTRY
        );
    }
}
