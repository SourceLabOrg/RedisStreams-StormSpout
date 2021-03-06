package org.sourcelab.storm.spout.redis;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sourcelab.storm.spout.redis.client.Client;
import org.sourcelab.storm.spout.redis.client.ClientFactory;
import org.sourcelab.storm.spout.redis.client.Consumer;
import org.sourcelab.storm.spout.redis.client.lettuce.LettuceClient;
import org.sourcelab.storm.spout.redis.funnel.ConsumerFunnel;
import org.sourcelab.storm.spout.redis.funnel.MemoryFunnel;
import org.sourcelab.storm.spout.redis.funnel.SpoutFunnel;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Redis Stream based Spout for Apache Storm 2.2.x.
 */
public class RedisStreamSpout implements IRichSpout, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(RedisStreamSpout.class);

    /**
     * Configuration Properties for the Spout.
     */
    private final RedisStreamSpoutConfig config;

    /**
     * Converts from a Message into a tuple.
     */
    private final TupleConverter messageConverter;

    /**
     * Topology context.
     */
    private transient TopologyContext topologyContext;

    /**
     * Storm Output Collector reference.
     */
    private transient SpoutOutputCollector collector;

    /**
     * Thread-Safe interface for passing messages between the Redis Stream thread and Spout Thread.
     */
    private transient SpoutFunnel funnel;

    /**
     * Background consumer thread.
     */
    private transient Thread consumerThread = null;

    /**
     * Constructor.
     * @param config Configuration properties for the spout.
     */
    public RedisStreamSpout(final RedisStreamSpoutConfig config) {
        this.config = Objects.requireNonNull(config);
        this.messageConverter = config.getTupleConverter();
    }

    /**
     * Constructor.
     * @param builder Configuration properties for the spout.
     */
    public RedisStreamSpout(final RedisStreamSpoutConfig.Builder builder) {
        this(Objects.requireNonNull(builder.build()));
    }

    @Override
    public void open(
        final Map<String, Object> spoutConfig,
        final TopologyContext topologyContext,
        final SpoutOutputCollector spoutOutputCollector
    ) {
        this.topologyContext = Objects.requireNonNull(topologyContext);
        this.collector = Objects.requireNonNull(spoutOutputCollector);

        // Create funnel instance.
        this.funnel = new MemoryFunnel(config, spoutConfig, topologyContext);

        // Create and start consumer thread.
        createAndStartConsumerThread();
    }

    @Override
    public void close() {
        // Request stop, this will block until the client thread has terminated.
        funnel.requestStop();
    }

    @Override
    public void activate() {
        // If the thread is already running and alive
        if (consumerThread != null && consumerThread.isAlive()) {
            // No-op.  It's already running, and deactivate() is a no-op for us.
            return;
        }

        // If we haven't created the consumer thread yet, or it has previously died.
        // Create and start it
        createAndStartConsumerThread();
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
        final TupleValue tuple = messageConverter.createTuple(nextMessage);
        if (tuple == null) {
            // If null returned, then we should ack the message and return
            funnel.ackMessage(nextMessage.getId());
            return;
        }

        // Emit down that stream.
        collector.emit(tuple.getStream(), tuple.getTuple(), nextMessage.getId());
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

        // Fail the msgId
        funnel.failMessage((String) msgId);
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
        for (final String stream : config.getTupleConverter().streams()) {
            final Fields fields = config.getTupleConverter().getFieldsFor(stream);
            declarer.declareStream(stream, fields);
        }
    }

    /**
     * Not used.
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return new HashMap<>();
    }

    /**
     * Create background consumer thread.
     */
    private void createAndStartConsumerThread() {
        // Create consumer and client
        final int taskIndex = topologyContext.getThisTaskIndex();
        final Client client = new ClientFactory().createClient(config, taskIndex);
        final Consumer consumer = new Consumer(config, client, (ConsumerFunnel) funnel);

        // Create background consuming thread.
        consumerThread = new Thread(
            consumer,
            "RedisStreamSpout-ConsumerThread[" + taskIndex + "]"
        );
        consumerThread.start();
    }
}
