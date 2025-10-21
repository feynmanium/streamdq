package com.streamdq.window;

import com.streamdq.api.CheckResult;
import com.streamdq.api.CheckStatus;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.function.Function;

/**
 * Windowed completeness check that aggregates completeness over a time window.
 * This is more appropriate for streaming than checking individual records.
 *
 * @param <T> The type of data being checked
 * @param <W> The type of window
 */
public class WindowedCompletenessCheck<T, W extends Window> {
    private final String fieldName;
    private final Function<T, Object> fieldExtractor;
    private final double threshold; // Minimum acceptable completeness rate (0.0 to 1.0)

    public WindowedCompletenessCheck(String fieldName, Function<T, Object> fieldExtractor, double threshold) {
        this.fieldName = fieldName;
        this.fieldExtractor = fieldExtractor;
        this.threshold = threshold;
    }

    /**
     * Apply this check to a windowed stream and return CheckResults per window.
     */
    public DataStream<CheckResult> apply(WindowedStream<T, ?, W> windowedStream) {
        return windowedStream.aggregate(new CompletenessAggregator());
    }

    private class CompletenessAggregator implements AggregateFunction<T, CompletenessAccumulator, CheckResult> {
        private static final long serialVersionUID = 1L;

        @Override
        public CompletenessAccumulator createAccumulator() {
            return new CompletenessAccumulator();
        }

        @Override
        public CompletenessAccumulator add(T value, CompletenessAccumulator accumulator) {
            accumulator.total++;
            Object fieldValue = fieldExtractor.apply(value);
            if (fieldValue != null) {
                accumulator.complete++;
            }
            return accumulator;
        }

        @Override
        public CheckResult getResult(CompletenessAccumulator accumulator) {
            double completenessRate = accumulator.total > 0
                ? (double) accumulator.complete / accumulator.total
                : 1.0;

            CheckResult.Builder builder = CheckResult.builder("WindowedCompleteness_" + fieldName)
                .metricValue(completenessRate)
                .addMetadata("field", fieldName)
                .addMetadata("threshold", threshold)
                .addMetadata("total_records", accumulator.total)
                .addMetadata("complete_records", accumulator.complete);

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
            return a;
        }
    }

    private static class CompletenessAccumulator {
        long total = 0;
        long complete = 0;
    }

    public static <T, W extends Window> WindowedCompletenessCheck<T, W> on(
        String fieldName,
        Function<T, Object> fieldExtractor,
        double threshold) {
        return new WindowedCompletenessCheck<>(fieldName, fieldExtractor, threshold);
    }
}
