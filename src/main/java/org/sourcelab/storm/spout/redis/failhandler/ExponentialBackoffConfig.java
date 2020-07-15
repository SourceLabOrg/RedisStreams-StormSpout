package org.sourcelab.storm.spout.redis.failhandler;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Configuration for {@link ExponentialBackoffFailureHandler}.
 */
public class ExponentialBackoffConfig implements Serializable {
    /**
     * Define our retry limit.
     * A value of less than 0 will mean we'll retry forever
     * A value of 0 means we'll never retry.
     * A value of greater than 0 sets an upper bound of number of retries.
     */
    private final int retryLimit;

    /**
     * Initial delay after a tuple fails for the first time, in milliseconds.
     */
    private final long initialRetryDelayMs;

    /**
     * Each time we fail, double our delay, so 4, 8, 16 seconds, etc.
     */
    private final double retryDelayMultiplier;

    /**
     * Maximum delay between successive retries, defaults to 15 minutes.
     */
    private final long retryDelayMaxMs;

    /**
     * Enable/Disable flag for collecting metrics.
     * Defaults to enabled.
     */
    private final boolean metricsEnabled;

    /**
     * Constructor.  See Builder instance.
     */
    public ExponentialBackoffConfig(
        final int retryLimit,
        final long initialRetryDelayMs,
        final double retryDelayMultiplier,
        final long retryDelayMaxMs,
        final boolean metricsEnabled
    ) {
        this.retryLimit = retryLimit;
        this.initialRetryDelayMs = initialRetryDelayMs;
        this.retryDelayMultiplier = retryDelayMultiplier;
        this.retryDelayMaxMs = retryDelayMaxMs;
        this.metricsEnabled = metricsEnabled;
    }

    /**
     * New Builder instance for ExponentialBackoffConfig.
     * @return New Builder instance for ExponentialBackoffConfig.
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Create a new default configuration.
     * @return Default configuration.
     */
    public static ExponentialBackoffConfig defaultConfig() {
        return new Builder().build();
    }

    public int getRetryLimit() {
        return retryLimit;
    }

    public long getInitialRetryDelayMs() {
        return initialRetryDelayMs;
    }

    public double getRetryDelayMultiplier() {
        return retryDelayMultiplier;
    }

    public long getRetryDelayMaxMs() {
        return retryDelayMaxMs;
    }

    public boolean isMetricsEnabled() {
        return metricsEnabled;
    }

    @Override
    public String toString() {
        return "ExponentialBackoffConfig{"
            + "retryLimit=" + retryLimit
            + ", initialRetryDelayMs=" + initialRetryDelayMs
            + ", retryDelayMultiplier=" + retryDelayMultiplier
            + ", retryDelayMaxMs=" + retryDelayMaxMs
            + ", metricsEnabled=" + metricsEnabled
            + '}';
    }

    /**
     * Builder for {@link ExponentialBackoffConfig}.
     */
    public static final class Builder {
        /**
         * Default retry limit is 10 attempts.
         */
        private int retryLimit = 10;

        /**
         * The first time a message has failed, defines the minimum delay.
         * Default initial delay is 2 seconds (2000 milliseconds).
         */
        private long initialRetryDelayMs = TimeUnit.SECONDS.toMillis(2);

        /**
         * Default multiplier to 2.0.
         */
        private double retryDelayMultiplier = 2.0;

        /**
         * Upperbound that we'll limit retries within.
         * Defaults to a max of 15 minutes.
         */
        private long retryDelayMaxMs = TimeUnit.MINUTES.toMillis(15);

        /**
         * Enable/Disable flag for collecting metrics.
         * Defaults to enabled.
         */
        private boolean metricsEnabled = true;

        private Builder() {
        }

        public Builder withRetryForever() {
            return withRetryLimit(-1);
        }

        public Builder withRetryNever() {
            return withRetryLimit(0);
        }

        public Builder withRetryLimit(int retryLimit) {
            this.retryLimit = retryLimit;
            return this;
        }

        public Builder withInitialRetryDelay(final Duration duration) {
            Objects.requireNonNull(duration);
            return withInitialRetryDelayMs(duration.toMillis());
        }

        public Builder withInitialRetryDelayMs(long initialRetryDelayMs) {
            this.initialRetryDelayMs = initialRetryDelayMs;
            return this;
        }

        public Builder withRetryDelayMultiplier(double retryDelayMultiplier) {
            this.retryDelayMultiplier = retryDelayMultiplier;
            return this;
        }

        public Builder withRetryDelayMax(final Duration duration) {
            Objects.requireNonNull(duration);
            return withRetryDelayMaxMs(duration.toMillis());
        }

        public Builder withRetryDelayMaxMs(long retryDelayMaxMs) {
            this.retryDelayMaxMs = retryDelayMaxMs;
            return this;
        }

        public Builder withMetricsDisabled() {
            return withMetricsEnabled(false);
        }

        public Builder withMetricsEnabled() {
            return withMetricsEnabled(true);
        }

        public Builder withMetricsEnabled(final boolean enabled) {
            this.metricsEnabled = enabled;
            return this;
        }

        /**
         * Create new ExponentialBackoffConfig instance.
         * @return ExponentialBackoffConfig.
         */
        public ExponentialBackoffConfig build() {
            return new ExponentialBackoffConfig(retryLimit, initialRetryDelayMs, retryDelayMultiplier, retryDelayMaxMs, metricsEnabled);
        }
    }
}
