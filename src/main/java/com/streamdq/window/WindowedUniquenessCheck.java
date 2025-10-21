package com.streamdq.window;

import com.streamdq.api.CheckResult;
import com.streamdq.api.CheckStatus;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

/**
 * Windowed uniqueness check - validates that values are unique within a window.
 *
 * Critical for streaming DQ (from research):
 * "Window-based deduplication represents a critical compromise in streaming architectures.
 * Frameworks like Apache Flink implement deduplication using time-based or count-based windows,
 * maintaining state for records within the window period."
 *
 * This cannot match batch's complete historical deduplication but is the best
 * streaming can do without unbounded state growth.
 *
 * @param <T> The type of data being checked
 * @param <W> The type of window
 */
public class WindowedUniquenessCheck<T, W extends Window> {
    private final String fieldName;
    private final Function<T, Object> fieldExtractor;
    private final double uniquenessThreshold; // Min acceptable uniqueness rate (0.0 to 1.0)

    public WindowedUniquenessCheck(String fieldName,
                                  Function<T, Object> fieldExtractor,
                                  double uniquenessThreshold) {
        this.fieldName = fieldName;
        this.fieldExtractor = fieldExtractor;
        this.uniquenessThreshold = uniquenessThreshold;
    }

    /**
     * Apply uniqueness check to a windowed stream.
     */
    public DataStream<CheckResult> apply(WindowedStream<T, ?, W> windowedStream) {
        return windowedStream.aggregate(new UniquenessAggregator());
    }

    private class UniquenessAggregator implements AggregateFunction<T, UniquenessAccumulator, CheckResult> {
        private static final long serialVersionUID = 1L;

        @Override
        public UniquenessAccumulator createAccumulator() {
            return new UniquenessAccumulator();
        }

        @Override
        public UniquenessAccumulator add(T value, UniquenessAccumulator acc) {
            acc.total++;
            Object fieldValue = fieldExtractor.apply(value);

            if (fieldValue != null) {
                if (!acc.seenValues.add(fieldValue)) {
                    acc.duplicates++;
                }
            } else {
                acc.nullCount++;
            }

            return acc;
        }

        @Override
        public CheckResult getResult(UniquenessAccumulator acc) {
            long uniqueCount = acc.seenValues.size();
            double uniquenessRate = acc.total > 0
                ? (double) uniqueCount / (acc.total - acc.nullCount)
                : 1.0;

            CheckResult.Builder builder = CheckResult.builder("WindowedUniqueness_" + fieldName)
                .metricValue(uniquenessRate)
                .addMetadata("field", fieldName)
                .addMetadata("threshold", uniquenessThreshold)
                .addMetadata("total_records", acc.total)
                .addMetadata("unique_values", uniqueCount)
                .addMetadata("duplicates", acc.duplicates)
                .addMetadata("null_count", acc.nullCount);

            if (uniquenessRate >= uniquenessThreshold) {
                builder.status(CheckStatus.SUCCESS)
                    .description(String.format("Uniqueness %.2f%% meets threshold %.2f%%",
                        uniquenessRate * 100, uniquenessThreshold * 100));
            } else {
                builder.status(CheckStatus.ERROR)
                    .description(String.format("Uniqueness %.2f%% below threshold %.2f%% (%d duplicates)",
                        uniquenessRate * 100, uniquenessThreshold * 100, acc.duplicates));
            }

            return builder.build();
        }

        @Override
        public UniquenessAccumulator merge(UniquenessAccumulator a, UniquenessAccumulator b) {
            a.total += b.total;
            a.nullCount += b.nullCount;

            // Merge seen values - detect cross-partition duplicates
            for (Object value : b.seenValues) {
                if (!a.seenValues.add(value)) {
                    a.duplicates++;
                }
            }
            a.duplicates += b.duplicates;

            return a;
        }
    }

    private static class UniquenessAccumulator {
        long total = 0;
        long duplicates = 0;
        long nullCount = 0;
        Set<Object> seenValues = new HashSet<>();

        // Note: For very high cardinality, consider using probabilistic data structures
        // like HyperLogLog or Bloom filters to reduce memory footprint
    }

    public static <T, W extends Window> WindowedUniquenessCheck<T, W> on(
        String fieldName,
        Function<T, Object> fieldExtractor,
        double uniquenessThreshold) {
        return new WindowedUniquenessCheck<>(fieldName, fieldExtractor, uniquenessThreshold);
    }

    /**
     * Expect 100% unique values (no duplicates).
     */
    public static <T, W extends Window> WindowedUniquenessCheck<T, W> expectUnique(
        String fieldName,
        Function<T, Object> fieldExtractor) {
        return new WindowedUniquenessCheck<>(fieldName, fieldExtractor, 1.0);
    }
}
