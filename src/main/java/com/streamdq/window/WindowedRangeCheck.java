package com.streamdq.window;

import com.streamdq.api.CheckResult;
import com.streamdq.api.CheckStatus;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.function.Function;

/**
 * Windowed range check that validates if values stay within bounds over a window.
 *
 * @param <T> The type of data being checked
 * @param <W> The type of window
 */
public class WindowedRangeCheck<T, W extends Window> {
    private final String fieldName;
    private final Function<T, Number> fieldExtractor;
    private final Double min;
    private final Double max;
    private final double violationThreshold; // Max acceptable violation rate (0.0 to 1.0)

    public WindowedRangeCheck(String fieldName,
                             Function<T, Number> fieldExtractor,
                             Double min,
                             Double max,
                             double violationThreshold) {
        this.fieldName = fieldName;
        this.fieldExtractor = fieldExtractor;
        this.min = min;
        this.max = max;
        this.violationThreshold = violationThreshold;
    }

    public DataStream<CheckResult> apply(WindowedStream<T, ?, W> windowedStream) {
        return windowedStream.aggregate(new RangeAggregator());
    }

    private class RangeAggregator implements AggregateFunction<T, RangeAccumulator, CheckResult> {
        private static final long serialVersionUID = 1L;

        @Override
        public RangeAccumulator createAccumulator() {
            return new RangeAccumulator();
        }

        @Override
        public RangeAccumulator add(T value, RangeAccumulator accumulator) {
            accumulator.total++;
            Number fieldValue = fieldExtractor.apply(value);

            if (fieldValue != null) {
                double doubleValue = fieldValue.doubleValue();
                accumulator.sum += doubleValue;
                accumulator.min = Math.min(accumulator.min, doubleValue);
                accumulator.max = Math.max(accumulator.max, doubleValue);

                if (doubleValue < min || doubleValue > max) {
                    accumulator.violations++;
                }
            } else {
                accumulator.nullCount++;
            }

            return accumulator;
        }

        @Override
        public CheckResult getResult(RangeAccumulator accumulator) {
            double violationRate = accumulator.total > 0
                ? (double) accumulator.violations / accumulator.total
                : 0.0;

            CheckResult.Builder builder = CheckResult.builder("WindowedRange_" + fieldName)
                .metricValue(1.0 - violationRate)
                .addMetadata("field", fieldName)
                .addMetadata("expected_min", min)
                .addMetadata("expected_max", max)
                .addMetadata("actual_min", accumulator.min)
                .addMetadata("actual_max", accumulator.max)
                .addMetadata("total_records", accumulator.total)
                .addMetadata("violations", accumulator.violations)
                .addMetadata("null_count", accumulator.nullCount);

            if (violationRate <= violationThreshold) {
                builder.status(CheckStatus.SUCCESS)
                    .description(String.format("Range violation rate %.2f%% within threshold %.2f%%",
                        violationRate * 100, violationThreshold * 100));
            } else {
                builder.status(CheckStatus.ERROR)
                    .description(String.format("Range violation rate %.2f%% exceeds threshold %.2f%%",
                        violationRate * 100, violationThreshold * 100));
            }

            return builder.build();
        }

        @Override
        public RangeAccumulator merge(RangeAccumulator a, RangeAccumulator b) {
            a.total += b.total;
            a.violations += b.violations;
            a.nullCount += b.nullCount;
            a.sum += b.sum;
            a.min = Math.min(a.min, b.min);
            a.max = Math.max(a.max, b.max);
            return a;
        }
    }

    private static class RangeAccumulator {
        long total = 0;
        long violations = 0;
        long nullCount = 0;
        double sum = 0.0;
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
    }

    public static <T, W extends Window> WindowedRangeCheck<T, W> on(
        String fieldName,
        Function<T, Number> fieldExtractor,
        Double min,
        Double max,
        double violationThreshold) {
        return new WindowedRangeCheck<>(fieldName, fieldExtractor, min, max, violationThreshold);
    }
}
