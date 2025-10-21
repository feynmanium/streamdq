package com.streamdq.time;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.io.Serializable;
import java.time.Duration;

/**
 * Configuration for event time and watermark handling in data quality checks.
 * This is critical for streaming data quality as it enables:
 * - Correct handling of out-of-order data
 * - Proper window boundaries based on event occurrence time
 * - Configurable lateness tolerance
 */
public class EventTimeConfig<T> implements Serializable {
    private static final long serialVersionUID = 1L;

    private final WatermarkStrategy<T> watermarkStrategy;
    private final Duration allowedLateness;
    private final boolean dropLateEvents;

    private EventTimeConfig(Builder<T> builder) {
        this.watermarkStrategy = builder.watermarkStrategy;
        this.allowedLateness = builder.allowedLateness != null ? builder.allowedLateness : Duration.ZERO;
        this.dropLateEvents = builder.dropLateEvents;
    }

    public WatermarkStrategy<T> getWatermarkStrategy() {
        return watermarkStrategy;
    }

    public Duration getAllowedLateness() {
        return allowedLateness;
    }

    public boolean shouldDropLateEvents() {
        return dropLateEvents;
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    public static class Builder<T> {
        private WatermarkStrategy<T> watermarkStrategy;
        private Duration allowedLateness = Duration.ZERO;
        private boolean dropLateEvents = false;

        /**
         * Set the watermark strategy for event time processing.
         */
        public Builder<T> watermarkStrategy(WatermarkStrategy<T> strategy) {
            this.watermarkStrategy = strategy;
            return this;
        }

        /**
         * Configure how long to wait for late data after the watermark.
         * Default is 0 (no lateness allowed).
         *
         * Example: Duration.ofMinutes(5) allows data up to 5 minutes late.
         */
        public Builder<T> allowedLateness(Duration lateness) {
            this.allowedLateness = lateness;
            return this;
        }

        /**
         * Whether to drop late events or send them to side output.
         * Default is false (send to side output).
         */
        public Builder<T> dropLateEvents(boolean drop) {
            this.dropLateEvents = drop;
            return this;
        }

        public EventTimeConfig<T> build() {
            if (watermarkStrategy == null) {
                throw new IllegalStateException("Watermark strategy must be set");
            }
            return new EventTimeConfig<>(this);
        }
    }
}
