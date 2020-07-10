package org.sourcelab.storm.spout.redis;

import org.apache.storm.spout.ISpout;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sourcelab.storm.spout.redis.client.Client;
import org.sourcelab.storm.spout.redis.client.LettuceClient;
import org.sourcelab.storm.spout.redis.funnel.ConsumerFunnel;
import org.sourcelab.storm.spout.redis.funnel.MemoryFunnel;
import org.sourcelab.storm.spout.redis.funnel.SpoutFunnel;
import org.sourcelab.storm.spout.redis.util.FactoryUtil;
import org.sourcelab.storm.spout.redis.util.ConfigUtil;

import java.util.Map;
import java.util.Objects;

/**
 * Redis Stream based Spout for Apache Storm 2.2.x.
 */
public class RedisStreamSpout implements ISpout {
    private static final Logger logger = LoggerFactory.getLogger(RedisStreamSpout.class);

    /**
     * Topology context.
     */
    private transient TopologyContext topologyContext;

    /**
     * Storm Output Collector reference.
     */
    private SpoutOutputCollector collector;

    /**
     * Converts from a Message into a tuple.
     */
    private TupleConverter messageConverter;

    /**
     * Underlying Client instance.
     */
    private Client client;

    /**
     * Thread-Safe interface for passing messages between the Redis Stream thread and Spout Thread.
     */
    private SpoutFunnel funnel;

    /**
     * Background consumer thread.
     */
    private Thread consumerThread = null;

    @Override
    public void open(
        final Map<String, Object> spoutConfig,
        final TopologyContext topologyContext,
        final SpoutOutputCollector spoutOutputCollector
    ) {
        this.topologyContext = Objects.requireNonNull(topologyContext);
        this.collector = Objects.requireNonNull(spoutOutputCollector);

        // Create config
        final Configuration config = ConfigUtil.load(
            spoutConfig, topologyContext
        );

        // Create message converter instance.
        messageConverter = FactoryUtil.newTupleConverter(config.getTupleConverterClass());

        // Create funnel instance.
        this.funnel = new MemoryFunnel(config, spoutConfig);

        // Create and connect client
        client = new LettuceClient(config, (ConsumerFunnel) funnel);
    }

    @Override
    public void close() {
        // Request stop, this will block until the client thread has terminated.
        funnel.requestStop();
    }

    @Override
    public void activate() {
        if (consumerThread != null) {
            // No-op.  It's already running, and deactivate() is a no-op for us.
            return;
        }
        // Start thread, this should return immediately, but start a background processing thread.
        // Create and start thread.
        consumerThread = new Thread(
            client,
            "RedisStreamSpout-ConsumerThread"
        );
        consumerThread.start();
    }

    @Override
    public void deactivate() {
        // Not implemented.  Background thread will consume until buffer is full and then block.
    }

    @Override
    public void nextTuple() {
        // Pop next message from funnel.
        final Message nextMessage = funnel.nextMessage();

        // If the funnel has no message
        if (nextMessage == null) {
            // Nothing to do.
            return;
        }

        // Build tuple from the message.
        final Values tuple = messageConverter.createTuple(nextMessage);
        if (tuple == null) {
            // If null returned, then we should ack the message and return
            funnel.ackMessage(nextMessage.getId());
            return;
        }

        // Get output stream.
        final String streamId = messageConverter.getStreamId(nextMessage);

        // If we have a stream Id.
        if (streamId != null) {
            // Emit down that stream.
            collector.emit(streamId, tuple, nextMessage.getId());
        } else {
            collector.emit(tuple, nextMessage.getId());
        }
    }

    @Override
    public void ack(final Object msgId) {
        // Ignore null
        if (msgId == null) {
            return;
        }

        // Ignore non-string msgIds
        if (!(msgId instanceof String)) {
            return;
        }

        // Ack the msgId.
        funnel.ackMessage((String) msgId);
    }

    @Override
    public void fail(final Object msgId) {
        // Ignore null
        if (msgId == null) {
            return;
        }

        // Ignore non-string msgIds
        if (!(msgId instanceof String)) {
            return;
        }

        // Fail the msgId.
        if (!funnel.failMessage((String) msgId)) {
            funnel.ackMessage((String) msgId);
        }
    }
}
