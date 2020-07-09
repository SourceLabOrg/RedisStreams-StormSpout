package org.sourcelab.storm.spout.redis;

import org.sourcelab.storm.spout.redis.funnel.Message;

import java.util.List;

/**
 * Used to convert from a Redis Message into a tuple.
 */
public interface TupleConverter {
    List<Object> createTuple(final Message message);

    String getStreamId(final Message message);
}
