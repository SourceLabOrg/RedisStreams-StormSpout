package org.sourcelab.storm.spout.redis.util.outputcollector;

import java.util.List;

/**
 * Used with StubSpoutCollector for testing.
 */
public class EmittedTuple {
    private final String streamId;
    private final List<Object> tuple;
    private final Object messageId;

    public EmittedTuple(final String streamId, final List<Object> tuple, final Object messageId) {
        this.streamId = streamId;
        this.tuple = tuple;
        this.messageId = messageId;
    }

    public String getStreamId() {
        return streamId;
    }

    public List<Object> getTuple() {
        return tuple;
    }

    public Object getMessageId() {
        return messageId;
    }

    @Override
    public String toString() {
        return "EmittedTuple{"
            + "streamId='" + streamId + '\''
            + ", tuple=" + tuple
            + ", messageId='" + messageId + '\''
            + '}';
    }
}
