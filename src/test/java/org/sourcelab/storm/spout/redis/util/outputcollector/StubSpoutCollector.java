package org.sourcelab.storm.spout.redis.util.outputcollector;

import org.apache.storm.spout.ISpoutOutputCollector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Stub implementation for testing.
 */
public class StubSpoutCollector implements ISpoutOutputCollector {

    final List<EmittedTuple> emittedTuples = new ArrayList<>();

    @Override
    public List<Integer> emit(final String streamId, final List<Object> tuple, final Object messageId) {
        emittedTuples.add(
            new EmittedTuple(streamId, tuple, messageId)
        );

        // Dummy value.
        return Collections.singletonList(1);
    }

    @Override
    public void emitDirect(final int taskId, final String streamId, final List<Object> tuple, final Object messageId) {
        throw new RuntimeException("Not implemented yet!");
    }

    @Override
    public long getPendingCount() {
        throw new RuntimeException("Not implemented yet!");
    }

    @Override
    public void flush() {
        throw new RuntimeException("Not implemented yet!");
    }

    @Override
    public void reportError(final Throwable error) {
        throw new RuntimeException("Not implemented yet!");
    }

    public List<EmittedTuple> getEmittedTuples() {
        return emittedTuples;
    }

    public void reset() {
        emittedTuples.clear();
    }
}
