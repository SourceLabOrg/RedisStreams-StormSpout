package org.sourcelab.storm.spout.redis;

import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * Used to convert from a Redis Message into a tuple.
 */
public interface TupleConverter extends Serializable {
    /**
     * Create a Tuple from the Message pulled from Redis Stream.
     * @param message Message pulled from Redis Stream.
     * @return Values/Tuple representation to be emitted by the spout.
     *         A return value of NULL means the message will be acked no tuple emitted by the spout.
     */
    TupleValue createTuple(final Message message);

    /**
     * Get the fields associated with a stream.  The streams passed in those
     * defined by the {@link TupleConverter#streams() } method.
     * @param stream the stream the fields are for
     * @return the fields for that stream.
     */
    Fields getFieldsFor(final String stream);

    /**
     * Get the list of streams this translator will handle.
     * @return the list of streams that this will handle.
     */
    default List<String> streams() {
        return Collections.singletonList(Utils.DEFAULT_STREAM_ID);
    }
}
