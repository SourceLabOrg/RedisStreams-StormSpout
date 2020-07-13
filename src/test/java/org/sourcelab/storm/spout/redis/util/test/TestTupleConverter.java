package org.sourcelab.storm.spout.redis.util.test;

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
    public List<Object> createTuple(final Message message) {
        final List<Object> values = new ArrayList<>();
        values.add(message.getId());

        // Hash Ordering is non-deterministic. So this is a terrible implementation.
        values.addAll(message.getMessage().values());

        return values;
    }

    @Override
    public String getStreamId(final Message message) {
        return null;
    }
}
