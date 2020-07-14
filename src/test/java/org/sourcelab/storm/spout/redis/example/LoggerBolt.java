package org.sourcelab.storm.spout.redis.example;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

/**
 * Simple debugging bolt that logs and acks tuples.
 */
public class LoggerBolt extends BaseRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(LoggerBolt.class);
    private transient OutputCollector collector;
    private transient Random random;

    @Override
    public void prepare(final Map<String, Object> config, final TopologyContext context, final OutputCollector collector) {
        this.collector = collector;
        this.random = new Random();
    }

    @Override
    public void execute(final Tuple input) {
        logger.info("Tuple: {}", input);

        // Periodically fail a tuple
        if (random.nextInt(10) == 0) {
            collector.fail(input);
            return;
        }

        // Otherwise just ack it.
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
    }
}
