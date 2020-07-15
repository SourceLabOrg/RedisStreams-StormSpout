package org.sourcelab.storm.spout.redis.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.metric.LoggingMetricsConsumer;
import org.apache.storm.topology.TopologyBuilder;
import org.sourcelab.storm.spout.redis.RedisStreamSpout;
import org.sourcelab.storm.spout.redis.RedisStreamSpoutConfig;
import org.sourcelab.storm.spout.redis.failhandler.ExponentialBackoffConfig;
import org.sourcelab.storm.spout.redis.failhandler.ExponentialBackoffFailureHandler;
import org.sourcelab.storm.spout.redis.util.test.RedisTestHelper;
import org.testcontainers.containers.GenericContainer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Example topology using the RedisStreamSpout deployed against a LocalTopology cluster.
 */
public class ExampleLocalTopology {
    private final GenericContainer redis;
    private Thread producerThread;

    /**
     * Main method.
     */
    public static void main(final String[] args) throws Exception {
        boolean enableDebug = false;
        if (args.length > 1 && "debug".equalsIgnoreCase(args[1])) {
            enableDebug = true;
        }

        // Create and run.
        new ExampleLocalTopology()
            .runExample(enableDebug);
    }

    /**
     * Constructor.
     */
    public ExampleLocalTopology() {
        // Setup REDIS Container.
        redis = new GenericContainer<>("redis:5.0.3-alpine")
            .withExposedPorts(6379);
    }

    /**
     * Fire up the topology against a local cluster.
     * @param enableDebug Enable debug logs?
     */
    public void runExample(final boolean enableDebug) throws Exception {
        try {
            // Start the redis container.
            redis.start();

            // Generate a random stream key
            final String streamKey = "MyStreamKey" + System.currentTimeMillis();
            final String groupName = "MyConsumerGroup";
            final String consumerPrefix = "StormConsumer";

            // Create config
            final RedisStreamSpoutConfig.Builder configBuilder = RedisStreamSpoutConfig.newBuilder()
                // Set Connection Properties
                .withHost(redis.getHost())
                .withPort(redis.getFirstMappedPort())
                // Consumer Properties
                .withGroupName(groupName)
                .withConsumerIdPrefix(consumerPrefix)
                .withStreamKey(streamKey)
                // Failure Handler
                .withFailureHandler(new ExponentialBackoffFailureHandler(ExponentialBackoffConfig.defaultConfig()))
                // Tuple Handler Class
                .withTupleConverter(new TestTupleConverter("value"));

            // Create topology
            final StormTopology exampleTopology = createTopology(configBuilder);

            // Create local cluster
            final LocalCluster localCluster = new LocalCluster();

            // Create configuration
            final Config config = new Config();
            config.setDebug(enableDebug);
            // Never fall back to java serialization
            config.setFallBackOnJavaSerialization(false);
            // Logging metrics consumer for metrics validation.
            config.registerMetricsConsumer(LoggingMetricsConsumer.class);

            // Submits the topology to the local cluster
            localCluster.submitTopology(
                "redis-stream-spout-example",
                config,
                exampleTopology
            );

            // Create a producer thread
            startProducerThread(streamKey);

            // Run until told to stop.
            stopWaitingForInput();
        } finally {
            // Shutdown producer thread.
            if (producerThread != null && producerThread.isAlive()) {
                producerThread.interrupt();
            }

            // Shutdown redis container
            redis.stop();
        }
        System.exit(0);
    }

    /**
     * Fire up the background producer thread.
     * @param streamKey key to produce to.
     */
    private void startProducerThread(final String streamKey) {
        final Runnable runnable = () -> {
            // Create helper
            final RedisTestHelper testHelper = new RedisTestHelper("redis://" + redis.getHost() + ":" + redis.getFirstMappedPort());

            long tupleCounter = 0L;
            do {
                // Construct a message
                final Map<String, String> msg = new HashMap<>();
                msg.put("value", "Tuple#" + tupleCounter++);

                // Produce the message.
                testHelper.produceMessage(streamKey, msg);

                // Sleep for a bit
                try {
                    Thread.sleep(1000L);
                } catch (final InterruptedException exception) {
                    break;
                }
            } while(true);
        };

        // Create and start thread.
        producerThread = new Thread(runnable);
        producerThread.start();
    }

    private StormTopology createTopology(final RedisStreamSpoutConfig.Builder spoutConfig) {
        final TopologyBuilder topologyBuilder = new TopologyBuilder();
        // Add our Spout
        topologyBuilder.setSpout("redis_stream_spout", new RedisStreamSpout(spoutConfig), 1);

        // Add logging bolt for debugging.
        topologyBuilder.setBolt("logger_bolt", new LoggerBolt())
            .shuffleGrouping("redis_stream_spout");
        return topologyBuilder.createTopology();
    }

    private void stopWaitingForInput() {
        try {
            System.out.println("PRESS ENTER TO STOP");
            new BufferedReader(new InputStreamReader(System.in)).readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
