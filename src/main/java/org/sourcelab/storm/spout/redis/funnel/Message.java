package org.sourcelab.storm.spout.redis.funnel;

import java.util.Map;
import java.util.Objects;

/**
 * Intermediate object representing the Redis Stream message, used to pass
 * between the Spout instance (prior to converting into a Tuple) and the background
 * consumer thread.
 */
public class Message {
    private final String id;
    private final Map<String, String> message;

    /**
     * Constructor.
     * @param id Id/offset of the message.
     * @param message The stream message.
     */
    public Message(final String id, final Map<String, String> message) {
        this.id = Objects.requireNonNull(id);
        this.message = Objects.requireNonNull(message);
    }

    public String getId() {
        return id;
    }

    public Map<String, String> getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return "Message{"
            + "id='" + id + '\''
            + ", message=" + message
            + '}';
    }
}
