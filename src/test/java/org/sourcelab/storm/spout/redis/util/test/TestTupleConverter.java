package org.sourcelab.storm.spout.redis.util.test;

import org.apache.storm.tuple.Fields;
import org.sourcelab.storm.spout.redis.TupleConverter;
import org.sourcelab.storm.spout.redis.Message;
import org.sourcelab.storm.spout.redis.TupleValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Test Implementation.
 */
public class TestTupleConverter implements TupleConverter {
    private final String[] fieldNames;

    /**
     * Constructor.
     * @param fieldNames name of the fields to pull.
     */
    public TestTupleConverter(final String ... fieldNames) {
        this.fieldNames = fieldNames;
    }

    @Override
    public TupleValue createTuple(final Message message) {
        final List<Object> values = new ArrayList<>();
        // MsgId
        values.add(message.getId());

        // Values
        for (final String fieldName : fieldNames) {
            if (!message.getBody().containsKey(fieldName)) {
                // Return null, no output tuple will be generated.
                return null;
            }
            values.add(message.getBody().get(fieldName));
        }

        // Create tuple.
        return new TupleValue(values);
    }

    @Override
    public Fields getFieldsFor(final String stream) {
        // Copy defined field names
        final List<String> fields = new ArrayList<>(Arrays.asList(fieldNames));

        // Prepend "msgId" field.
        fields.add(0, "msgId");

        // Return fields definition
        return new Fields(fields);
    }
}
