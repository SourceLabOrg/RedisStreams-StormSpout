package org.sourcelab.storm.spout.redis;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Intermediate object representing the Redis Stream message, used to pass
 * between the Spout instance (prior to converting into a Tuple) and the background
 * consumer thread.
 */
public class Message {
    private final String id;
    private final Map<String, String> body;

    /**
     * Constructor.
     * @param id Id/offset of the message.
     * @param body The stream message.
     */
    public Message(final String id, final Map<String, String> body) {
        this.id = Objects.requireNonNull(id);
        this.body = Collections.unmodifiableMap(Objects.requireNonNull(body));
    }

    public String getId() {
        return id;
    }

    public Map<String, String> getBody() {
        return body;
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        final Message message = (Message) other;
        return getId().equals(message.getId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "Message{"
            + "id='" + id + '\''
            + ", body=" + body
            + '}';
    }
}
