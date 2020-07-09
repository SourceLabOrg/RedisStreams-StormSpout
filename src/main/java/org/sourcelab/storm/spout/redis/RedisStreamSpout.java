package org.sourcelab.storm.spout.redis;

import org.apache.storm.spout.ISpout;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sourcelab.storm.spout.redis.client.Client;
import org.sourcelab.storm.spout.redis.client.LettuceClient;
import org.sourcelab.storm.spout.redis.funnel.ConsumerFunnel;
import org.sourcelab.storm.spout.redis.funnel.MemoryFunnel;
import org.sourcelab.storm.spout.redis.funnel.Message;
import org.sourcelab.storm.spout.redis.funnel.SpoutFunnel;
import org.sourcelab.storm.spout.redis.util.FactoryUtil;
import org.sourcelab.storm.spout.redis.util.StormToClientConfigurationUtil;

import java.util.List;
import java.util.Map;

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
        this.topologyContext = topologyContext;
        this.collector = spoutOutputCollector;

        // Create config
        final ClientConfiguration config = StormToClientConfigurationUtil.load(
            spoutConfig, topologyContext
        );

        // Create message converter instance.
        messageConverter = FactoryUtil.createNewInstance(config.getTupleConverterClass());

        // Create funnel instance.
        this.funnel = new MemoryFunnel(config);

        // Create and connect client
        client = new LettuceClient(config, (ConsumerFunnel) funnel);
    }

    @Override
    public void close() {
        // Request stop
        funnel.requestStop();
    }

    @Override
    public synchronized void activate() {
        // Start thread, this should return immediately, but start a background thread.
        if (consumerThread != null) {
            throw new IllegalStateException("Cannot call run() more than once!");
        }

        // Create and start thread.
        consumerThread = new Thread(
            client,
            "RedisStreamSpout-ConsumerThread"
        );
        consumerThread.start();
    }

    @Override
    public void deactivate() {
        // Pause consumption.
        // Not implemented.  Will consume until buffer is full.
    }

    @Override
    public void nextTuple() {
        // Pop next tuple from stream.
        final Message nextMessage = funnel.nextTuple();
        if (nextMessage == null) {
            return;
        }

        // Build tuple.
        final List<Object> tuple = messageConverter.createTuple(nextMessage);
        if (tuple == null) {
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

        // Track the msgId.
        funnel.ackTuple((String) msgId);
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

        // Track the msgId.
        funnel.failTuple((String) msgId);
    }
}
