package com.streamdq.window;

import com.streamdq.api.CheckResult;
import com.streamdq.api.CheckStatus;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

import java.time.Instant;
import java.util.function.Function;

/**
 * Event-time aware windowed completeness check with late data handling.
 *
 * This is critical for streaming DQ as it:
 * - Uses event time (when events occurred) not processing time
 * - Handles out-of-order data correctly
 * - Supports allowed lateness and late data side outputs
 * - Provides accurate completeness metrics even with delayed data
 *
 * @param <T> The type of data being checked
 */
public class EventTimeWindowedCheck<T> {
    private final String fieldName;
    private final Function<T, Object> fieldExtractor;
    private final double threshold;

    public EventTimeWindowedCheck(String fieldName, Function<T, Object> fieldExtractor, double threshold) {
        this.fieldName = fieldName;
        this.fieldExtractor = fieldExtractor;
        this.threshold = threshold;
    }

    /**
     * Apply this check to an event-time windowed stream.
     * Returns both regular results and late data via side output.
     */
    public WindowCheckResult<T> applyWithLateDataHandling(
        org.apache.flink.streaming.api.datastream.WindowedStream<T, ?, TimeWindow> windowedStream,
        OutputTag<T> lateDataTag) {

        SingleOutputStreamOperator<CheckResult> results = windowedStream
            .sideOutputLateData(lateDataTag)
            .aggregate(new EventTimeCompletenessAggregator());

        return new WindowCheckResult<>(results, lateDataTag);
    }

    private class EventTimeCompletenessAggregator
        implements AggregateFunction<T, CompletenessAccumulator, CheckResult> {
        private static final long serialVersionUID = 1L;

        @Override
        public CompletenessAccumulator createAccumulator() {
            return new CompletenessAccumulator();
        }

        @Override
        public CompletenessAccumulator add(T value, CompletenessAccumulator acc) {
            acc.total++;
            Object fieldValue = fieldExtractor.apply(value);
            if (fieldValue != null) {
                acc.complete++;
            }

            // Track first and last event time if available
            if (value instanceof EventTimeAware) {
                long eventTime = ((EventTimeAware) value).getEventTime();
                if (acc.minEventTime == 0) {
                    acc.minEventTime = eventTime;
                    acc.maxEventTime = eventTime;
                } else {
                    acc.minEventTime = Math.min(acc.minEventTime, eventTime);
                    acc.maxEventTime = Math.max(acc.maxEventTime, eventTime);
                }
            }

            return acc;
        }

        @Override
        public CheckResult getResult(CompletenessAccumulator acc) {
            double completenessRate = acc.total > 0
                ? (double) acc.complete / acc.total
                : 1.0;

            CheckResult.Builder builder = CheckResult.builder("EventTimeCompleteness_" + fieldName)
                .metricValue(completenessRate)
                .addMetadata("field", fieldName)
                .addMetadata("threshold", threshold)
                .addMetadata("total_records", acc.total)
                .addMetadata("complete_records", acc.complete);

            // Add event time span if available
            if (acc.minEventTime > 0) {
                builder.addMetadata("min_event_time", Instant.ofEpochMilli(acc.minEventTime).toString())
                    .addMetadata("max_event_time", Instant.ofEpochMilli(acc.maxEventTime).toString())
                    .addMetadata("event_time_span_ms", acc.maxEventTime - acc.minEventTime);
            }

            if (completenessRate >= threshold) {
                builder.status(CheckStatus.SUCCESS)
                    .description(String.format("Completeness %.2f%% meets threshold %.2f%%",
                        completenessRate * 100, threshold * 100));
            } else {
                builder.status(CheckStatus.ERROR)
                    .description(String.format("Completeness %.2f%% below threshold %.2f%%",
                        completenessRate * 100, threshold * 100));
            }

            return builder.build();
        }

        @Override
        public CompletenessAccumulator merge(CompletenessAccumulator a, CompletenessAccumulator b) {
            a.total += b.total;
            a.complete += b.complete;
            if (b.minEventTime > 0) {
                if (a.minEventTime == 0) {
                    a.minEventTime = b.minEventTime;
                    a.maxEventTime = b.maxEventTime;
                } else {
                    a.minEventTime = Math.min(a.minEventTime, b.minEventTime);
                    a.maxEventTime = Math.max(a.maxEventTime, b.maxEventTime);
                }
            }
            return a;
        }
    }

    private static class CompletenessAccumulator {
        long total = 0;
        long complete = 0;
        long minEventTime = 0;
        long maxEventTime = 0;
    }

    /**
     * Marker interface for elements that carry their event time.
     */
    public interface EventTimeAware {
        long getEventTime();
    }

    /**
     * Result container with access to late data side output.
     */
    public static class WindowCheckResult<T> {
        private final DataStream<CheckResult> results;
        private final OutputTag<T> lateDataTag;

        public WindowCheckResult(DataStream<CheckResult> results, OutputTag<T> lateDataTag) {
            this.results = results;
            this.lateDataTag = lateDataTag;
        }

        public DataStream<CheckResult> getResults() {
            return results;
        }

        public OutputTag<T> getLateDataTag() {
            return lateDataTag;
        }

        /**
         * Get the late data side output from the main stream operator.
         */
        public DataStream<T> getLateData(SingleOutputStreamOperator<?> mainStream) {
            return mainStream.getSideOutput(lateDataTag);
        }
    }

    public static <T> EventTimeWindowedCheck<T> on(
        String fieldName,
        Function<T, Object> fieldExtractor,
        double threshold) {
        return new EventTimeWindowedCheck<>(fieldName, fieldExtractor, threshold);
    }
}
