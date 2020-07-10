package org.sourcelab.storm.spout.redis.util;

import org.apache.storm.tuple.Values;
import org.sourcelab.storm.spout.redis.TupleConverter;
import org.sourcelab.storm.spout.redis.Message;

import java.util.ArrayList;
import java.util.List;

/**
 * NOT FOR PRODUCTION USE. Test Implementation.
 *
 * Because HashMap ordering is non-deterministic, the results
 * from this implementation is also non-deterministic.
 */
public class TestTupleConverter implements TupleConverter {
    @Override
    public Values createTuple(final Message message) {
        final List<String> values = new ArrayList<>();
        values.add(message.getId());

        // Hash Ordering is non-deterministic. So this is a terrible implementation.
        values.addAll(message.getMessage().values());

        return new Values(values);
    }

    @Override
    public String getStreamId(final Message message) {
        return null;
    }
}
