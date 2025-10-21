package com.streamdq.window;

import com.streamdq.api.CheckResult;
import com.streamdq.api.CheckStatus;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.function.Function;

/**
 * Windowed null rate check - tracks percentage of null values within windows.
 *
 * Null rate is inverse of completeness rate but provides different semantics:
 * - Completeness: 95% complete = 95% non-null (threshold: min acceptable)
 * - Null rate: 5% null (threshold: max acceptable)
 *
 * Use null rate when you want to alert on INCREASING nulls:
 * - "Alert if null rate exceeds 10%"
 * - Monitor for data pipeline degradation
 * - Detect upstream data quality issues
 *
 * @param <T> The type of data being checked
 * @param <W> The type of window
 */
public class WindowedNullRateCheck<T, W extends Window> {
    private final String fieldName;
    private final Function<T, Object> fieldExtractor;
    private final double maxNullRate; // Max acceptable null rate (0.0 to 1.0)

    public WindowedNullRateCheck(String fieldName,
                                Function<T, Object> fieldExtractor,
                                double maxNullRate) {
        this.fieldName = fieldName;
        this.fieldExtractor = fieldExtractor;
        this.maxNullRate = maxNullRate;
    }

    /**
     * Apply null rate check to a windowed stream.
     */
    public DataStream<CheckResult> apply(WindowedStream<T, ?, W> windowedStream) {
        return windowedStream.aggregate(new NullRateAggregator());
    }

    private class NullRateAggregator implements AggregateFunction<T, NullRateAccumulator, CheckResult> {
        private static final long serialVersionUID = 1L;

        @Override
        public NullRateAccumulator createAccumulator() {
            return new NullRateAccumulator();
        }

        @Override
        public NullRateAccumulator add(T value, NullRateAccumulator acc) {
            acc.total++;
            Object fieldValue = fieldExtractor.apply(value);

            if (fieldValue == null) {
                acc.nullCount++;
            }

            return acc;
        }

        @Override
        public CheckResult getResult(NullRateAccumulator acc) {
            double nullRate = acc.total > 0
                ? (double) acc.nullCount / acc.total
                : 0.0;

            CheckResult.Builder builder = CheckResult.builder("WindowedNullRate_" + fieldName)
                .metricValue(1.0 - nullRate) // Metric value is "quality" so invert null rate
                .addMetadata("field", fieldName)
                .addMetadata("max_null_rate", maxNullRate)
                .addMetadata("actual_null_rate", nullRate)
                .addMetadata("total_records", acc.total)
                .addMetadata("null_count", acc.nullCount)
                .addMetadata("non_null_count", acc.total - acc.nullCount);

            if (nullRate <= maxNullRate) {
                builder.status(CheckStatus.SUCCESS)
                    .description(String.format("Null rate %.2f%% within threshold %.2f%%",
                        nullRate * 100, maxNullRate * 100));
            } else {
                builder.status(CheckStatus.ERROR)
                    .description(String.format("Null rate %.2f%% exceeds threshold %.2f%% (%d nulls)",
                        nullRate * 100, maxNullRate * 100, acc.nullCount));
            }

            return builder.build();
        }

        @Override
        public NullRateAccumulator merge(NullRateAccumulator a, NullRateAccumulator b) {
            a.total += b.total;
            a.nullCount += b.nullCount;
            return a;
        }
    }

    private static class NullRateAccumulator {
        long total = 0;
        long nullCount = 0;
    }

    public static <T, W extends Window> WindowedNullRateCheck<T, W> on(
        String fieldName,
        Function<T, Object> fieldExtractor,
        double maxNullRate) {
        return new WindowedNullRateCheck<>(fieldName, fieldExtractor, maxNullRate);
    }

    /**
     * Expect zero nulls (100% complete).
     */
    public static <T, W extends Window> WindowedNullRateCheck<T, W> expectNoNulls(
        String fieldName,
        Function<T, Object> fieldExtractor) {
        return new WindowedNullRateCheck<>(fieldName, fieldExtractor, 0.0);
    }
}
