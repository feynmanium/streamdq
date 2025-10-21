package com.streamdq.time;

import com.streamdq.api.Check;
import com.streamdq.api.CheckResult;
import com.streamdq.api.CheckStatus;
import org.apache.flink.api.common.functions.MapFunction;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Function;

/**
 * Checks data timeliness - critical for streaming data quality.
 * Validates that events are processed within acceptable time bounds.
 *
 * Timeliness is a key data quality dimension in streaming that doesn't exist in batch:
 * - Detects processing lag
 * - Ensures data freshness
 * - Identifies upstream delays
 *
 * @param <T> The type of data being checked
 */
public class TimelinessCheck<T> implements Check<T> {
    private static final long serialVersionUID = 1L;

    private final String name;
    private final Function<T, Instant> eventTimeExtractor;
    private final Duration maxLatency;
    private final boolean useEventTime;

    /**
     * Create a timeliness check.
     *
     * @param name Check name
     * @param eventTimeExtractor Function to extract event timestamp
     * @param maxLatency Maximum acceptable latency (event time to processing time)
     * @param useEventTime If true, compare event time to processing time. If false, just check event time exists.
     */
    public TimelinessCheck(String name,
                          Function<T, Instant> eventTimeExtractor,
                          Duration maxLatency,
                          boolean useEventTime) {
        this.name = name;
        this.eventTimeExtractor = eventTimeExtractor;
        this.maxLatency = maxLatency;
        this.useEventTime = useEventTime;
    }

    @Override
    public String getName() {
        return "Timeliness_" + name;
    }

    @Override
    public String getDescription() {
        return String.format("Checks that events are processed within %s of event time",
            maxLatency);
    }

    @Override
    public MapFunction<T, CheckResult> toMapFunction() {
        return this::validate;
    }

    @Override
    public CheckResult validate(T element) {
        Instant eventTime = eventTimeExtractor.apply(element);

        if (eventTime == null) {
            return CheckResult.builder(getName())
                .status(CheckStatus.ERROR)
                .metricValue(0.0)
                .description("Event time is null - cannot check timeliness")
                .addMetadata("max_latency_ms", maxLatency.toMillis())
                .build();
        }

        if (!useEventTime) {
            // Just check that event time exists and is reasonable
            return CheckResult.builder(getName())
                .status(CheckStatus.SUCCESS)
                .metricValue(1.0)
                .description("Event time is present")
                .addMetadata("event_time", eventTime.toString())
                .build();
        }

        Instant processingTime = Instant.now();
        Duration latency = Duration.between(eventTime, processingTime);

        CheckResult.Builder builder = CheckResult.builder(getName())
            .addMetadata("event_time", eventTime.toString())
            .addMetadata("processing_time", processingTime.toString())
            .addMetadata("latency_ms", latency.toMillis())
            .addMetadata("max_latency_ms", maxLatency.toMillis());

        if (latency.compareTo(maxLatency) <= 0) {
            builder.status(CheckStatus.SUCCESS)
                .metricValue(1.0)
                .description(String.format("Event is timely (latency: %dms)", latency.toMillis()));
        } else {
            double violationRatio = (double) latency.toMillis() / maxLatency.toMillis();
            builder.status(CheckStatus.ERROR)
                .metricValue(1.0 / violationRatio) // Lower score for higher violation
                .description(String.format("Event is late (latency: %dms, max: %dms)",
                    latency.toMillis(), maxLatency.toMillis()));
        }

        return builder.build();
    }

    public static <T> TimelinessCheck<T> maxLatency(
        String name,
        Function<T, Instant> eventTimeExtractor,
        Duration maxLatency) {
        return new TimelinessCheck<>(name, eventTimeExtractor, maxLatency, true);
    }

    public static <T> TimelinessCheck<T> checkEventTimePresent(
        String name,
        Function<T, Instant> eventTimeExtractor) {
        return new TimelinessCheck<>(name, eventTimeExtractor, Duration.ZERO, false);
    }
}
