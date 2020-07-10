package org.sourcelab.storm.spout.redis;

import org.apache.storm.tuple.Values;

/**
 * Used to convert from a Redis Message into a tuple.
 */
public interface TupleConverter {
    /**
     * Create a Tuple from the Message pulled from Redis Stream.
     * @param message Message pulled from Redis Stream.
     * @return Values/Tuple representation to be emitted by the spout.
     *         A return value of NULL means the message will be acked no tuple emitted by the spout.
     */
    Values createTuple(final Message message);

    /**
     * Determine which stream to emit the tuple down.
     * @param message Message pulled from Redis Stream.
     * @return The name of a stream, or a value of NULL to emit down the default stream.
     */
    String getStreamId(final Message message);
}
