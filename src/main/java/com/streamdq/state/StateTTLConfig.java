package com.streamdq.state;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;

import java.io.Serializable;

/**
 * Configuration for State Time-To-Live in streaming data quality checks.
 *
 * Critical for streaming DQ because:
 * - Unbounded streams = potentially unbounded state
 * - Without TTL, state grows indefinitely causing OOM
 * - Long-running jobs need automatic state cleanup
 * - Balance between keeping historical context and memory usage
 */
public class StateTTLConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private final StateTtlConfig ttlConfig;
    private final Time ttl;

    private StateTTLConfig(Builder builder) {
        this.ttl = builder.ttl;
        this.ttlConfig = StateTtlConfig
            .newBuilder(builder.ttl)
            .setUpdateType(builder.updateType)
            .setStateVisibility(builder.stateVisibility)
            .build();
    }

    public StateTtlConfig getFlinkTtlConfig() {
        return ttlConfig;
    }

    public Time getTtl() {
        return ttl;
    }

    public static Builder builder(Time ttl) {
        return new Builder(ttl);
    }

    public static class Builder {
        private final Time ttl;
        private StateTtlConfig.UpdateType updateType = StateTtlConfig.UpdateType.OnCreateAndWrite;
        private StateTtlConfig.StateVisibility stateVisibility = StateTtlConfig.StateVisibility.NeverReturnExpired;

        private Builder(Time ttl) {
            this.ttl = ttl;
        }

        /**
         * Update TTL on read and write (default: only on write).
         * Useful for keeping frequently accessed state alive.
         */
        public Builder updateOnReadAndWrite() {
            this.updateType = StateTtlConfig.UpdateType.OnReadAndWrite;
            return this;
        }

        /**
         * Only update TTL on write (default).
         * More efficient but state may expire even if read recently.
         */
        public Builder updateOnWriteOnly() {
            this.updateType = StateTtlConfig.UpdateType.OnCreateAndWrite;
            return this;
        }

        /**
         * Return expired state if not yet cleaned (default: never return expired).
         * Can be useful for best-effort scenarios.
         */
        public Builder returnExpiredIfNotCleanedUp() {
            this.stateVisibility = StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp;
            return this;
        }

        public StateTTLConfig build() {
            return new StateTTLConfig(this);
        }
    }

    /**
     * Common TTL configurations for data quality use cases.
     */
    public static class Presets {
        /**
         * 1 hour TTL - good for high-frequency metrics.
         */
        public static StateTTLConfig oneHour() {
            return builder(Time.hours(1)).build();
        }

        /**
         * 24 hour TTL - good for daily aggregations.
         */
        public static StateTTLConfig oneDay() {
            return builder(Time.hours(24)).build();
        }

        /**
         * 7 day TTL - good for weekly patterns.
         */
        public static StateTTLConfig oneWeek() {
            return builder(Time.days(7)).build();
        }

        /**
         * 30 day TTL - good for monthly analytics.
         */
        public static StateTTLConfig oneMonth() {
            return builder(Time.days(30)).build();
        }

        /**
         * No TTL - state never expires (use with caution!).
         */
        public static StateTTLConfig noExpiry() {
            return null; // Indicates no TTL should be set
        }
    }
}
