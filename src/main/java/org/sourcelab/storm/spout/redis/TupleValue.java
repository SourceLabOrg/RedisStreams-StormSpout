package org.sourcelab.storm.spout.redis;

import org.apache.storm.utils.Utils;

import java.util.List;
import java.util.Objects;

/**
 * A converted Tuple to be emitted into the topology.
 */
public class TupleValue {
    private final List<Object> tuple;
    private final String stream;

    /**
     * Define a tuple to be emitted out the DEFAULT stream.
     * @param tuple Tuple values.
     */
    public TupleValue(final List<Object> tuple) {
        this(tuple, Utils.DEFAULT_STREAM_ID);
    }

    /**
     * Define a tuple to be emitted out the passed stream.
     * @param tuple Tuple values.
     * @param stream Name of the stream to emit out.
     */
    public TupleValue(final List<Object> tuple, final String stream) {
        this.tuple = Objects.requireNonNull(tuple);
        this.stream = Objects.requireNonNull(stream);
    }

    public List<Object> getTuple() {
        return tuple;
    }

    public String getStream() {
        return stream;
    }

    @Override
    public String toString() {
        return "TupleValue{"
            + "tuple=" + tuple
            + ", stream='" + stream + '\''
            + '}';
    }
}
