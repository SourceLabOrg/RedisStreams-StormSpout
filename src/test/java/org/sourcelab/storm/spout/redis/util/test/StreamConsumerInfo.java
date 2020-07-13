package org.sourcelab.storm.spout.redis.util.test;

/**
 * Attempts to provide a usable interface for xconsumerInfo result.
 */
public class StreamConsumerInfo {
    private final String consumerId;
    private final long pending;
    private final long idle;

    public StreamConsumerInfo(final String consumerId, final long pending, final long idle) {
        this.consumerId = consumerId;
        this.pending = pending;
        this.idle = idle;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public String getConsumerId() {
        return consumerId;
    }

    public long getPending() {
        return pending;
    }

    public long getIdle() {
        return idle;
    }

    @Override
    public String toString() {
        return "StreamConsumerInfo{"
            + "consumerId='" + consumerId + '\''
            + ", pending=" + pending
            + ", idle=" + idle
            + '}';
    }


    public static final class Builder {
        private String consumerId;
        private long pending = -1L;
        private long idle = -1L;

        private Builder() {
        }

        public Builder withConsumerId(String consumerId) {
            this.consumerId = consumerId;
            return this;
        }

        public Builder withPending(long pending) {
            this.pending = pending;
            return this;
        }

        public Builder withIdle(long idle) {
            this.idle = idle;
            return this;
        }

        public StreamConsumerInfo build() {
            return new StreamConsumerInfo(consumerId, pending, idle);
        }
    }
}
