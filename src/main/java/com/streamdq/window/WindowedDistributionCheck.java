package com.streamdq.window;

import com.streamdq.api.CheckResult;
import com.streamdq.api.CheckStatus;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Windowed distribution check - tracks value frequency distribution within windows.
 *
 * Critical for streaming DQ to detect:
 * - Unexpected value distributions (new categories appearing)
 * - Skewed distributions (one value dominating when should be balanced)
 * - Missing expected values
 * - Data quality degradation over time
 *
 * Example use cases:
 * - Monitor status code distribution (should see mix of 200, 404, 500)
 * - Validate product category balance
 * - Detect anomalous concentration in categorical data
 *
 * WARNING: For high cardinality fields (millions of distinct values),
 * this will consume significant memory. Use only for low/medium cardinality
 * categorical fields. For high cardinality, use WindowedCardinalityCheck instead.
 *
 * @param <T> The type of data being checked
 * @param <W> The type of window
 */
public class WindowedDistributionCheck<T, W extends Window> {
    private final String fieldName;
    private final Function<T, Object> fieldExtractor;
    private final Integer maxTopValues; // Limit to top N values to prevent memory explosion
    private final Double minValueFrequency; // Min frequency for any value (e.g., 0.01 = 1%)
    private final Double maxValueFrequency; // Max frequency for any single value (e.g., 0.9 = 90%)

    public WindowedDistributionCheck(String fieldName,
                                    Function<T, Object> fieldExtractor,
                                    Integer maxTopValues,
                                    Double minValueFrequency,
                                    Double maxValueFrequency) {
        this.fieldName = fieldName;
        this.fieldExtractor = fieldExtractor;
        this.maxTopValues = maxTopValues;
        this.minValueFrequency = minValueFrequency;
        this.maxValueFrequency = maxValueFrequency;
    }

    /**
     * Apply distribution check to a windowed stream.
     */
    public DataStream<CheckResult> apply(WindowedStream<T, ?, W> windowedStream) {
        return windowedStream.aggregate(new DistributionAggregator());
    }

    private class DistributionAggregator
        implements AggregateFunction<T, DistributionAccumulator, CheckResult> {
        private static final long serialVersionUID = 1L;

        @Override
        public DistributionAccumulator createAccumulator() {
            return new DistributionAccumulator();
        }

        @Override
        public DistributionAccumulator add(T value, DistributionAccumulator acc) {
            acc.total++;
            Object fieldValue = fieldExtractor.apply(value);

            if (fieldValue == null) {
                acc.nullCount++;
            } else {
                acc.valueCounts.merge(fieldValue, 1L, Long::sum);
            }

            return acc;
        }

        @Override
        public CheckResult getResult(DistributionAccumulator acc) {
            long nonNullCount = acc.total - acc.nullCount;

            CheckResult.Builder builder = CheckResult.builder("WindowedDistribution_" + fieldName)
                .addMetadata("field", fieldName)
                .addMetadata("total_records", acc.total)
                .addMetadata("distinct_values", acc.valueCounts.size())
                .addMetadata("null_count", acc.nullCount);

            // Find top values and their frequencies
            Map<Object, Double> frequencies = new HashMap<>();
            double maxFreq = 0.0;
            Object mostCommonValue = null;

            for (Map.Entry<Object, Long> entry : acc.valueCounts.entrySet()) {
                double freq = (double) entry.getValue() / nonNullCount;
                frequencies.put(entry.getKey(), freq);

                if (freq > maxFreq) {
                    maxFreq = freq;
                    mostCommonValue = entry.getKey();
                }
            }

            builder.addMetadata("most_common_value", mostCommonValue != null ? mostCommonValue.toString() : "N/A")
                .addMetadata("max_frequency", maxFreq);

            // Add top values (limited to prevent metadata explosion)
            int topCount = maxTopValues != null ? Math.min(maxTopValues, frequencies.size()) : Math.min(10, frequencies.size());
            builder.addMetadata("top_values_count", topCount);

            // Check constraints
            boolean violatesMax = maxValueFrequency != null && maxFreq > maxValueFrequency;
            boolean violatesMin = false;

            if (minValueFrequency != null) {
                for (double freq : frequencies.values()) {
                    if (freq < minValueFrequency && freq > 0) {
                        violatesMin = true;
                        break;
                    }
                }
            }

            // Determine status
            if (!violatesMax && !violatesMin) {
                builder.status(CheckStatus.SUCCESS)
                    .metricValue(1.0)
                    .description(String.format("Distribution healthy (%d distinct values, max freq %.2f%%)",
                        acc.valueCounts.size(), maxFreq * 100));
            } else {
                builder.status(CheckStatus.ERROR)
                    .metricValue(1.0 - maxFreq);  // Lower score for skewed distributions

                if (violatesMax) {
                    builder.description(String.format(
                        "Distribution skewed: value '%s' appears %.2f%% (max allowed %.2f%%)",
                        mostCommonValue, maxFreq * 100,
                        maxValueFrequency * 100));
                } else {
                    builder.description(String.format(
                        "Distribution has rare values below %.2f%% threshold",
                        minValueFrequency * 100));
                }
            }

            return builder.build();
        }

        @Override
        public DistributionAccumulator merge(DistributionAccumulator a, DistributionAccumulator b) {
            a.total += b.total;
            a.nullCount += b.nullCount;

            for (Map.Entry<Object, Long> entry : b.valueCounts.entrySet()) {
                a.valueCounts.merge(entry.getKey(), entry.getValue(), Long::sum);
            }

            return a;
        }
    }

    private static class DistributionAccumulator {
        long total = 0;
        long nullCount = 0;
        Map<Object, Long> valueCounts = new HashMap<>();
    }

    /**
     * Track distribution with max frequency constraint (prevent skew).
     */
    public static <T, W extends Window> WindowedDistributionCheck<T, W> maxFrequency(
        String fieldName,
        Function<T, Object> fieldExtractor,
        double maxFrequency) {
        return new WindowedDistributionCheck<>(fieldName, fieldExtractor, null, null, maxFrequency);
    }

    /**
     * Track distribution with balanced requirement (all values >= min frequency).
     */
    public static <T, W extends Window> WindowedDistributionCheck<T, W> minFrequency(
        String fieldName,
        Function<T, Object> fieldExtractor,
        double minFrequency) {
        return new WindowedDistributionCheck<>(fieldName, fieldExtractor, null, minFrequency, null);
    }

    /**
     * Just track distribution without constraints (monitoring only).
     */
    public static <T, W extends Window> WindowedDistributionCheck<T, W> track(
        String fieldName,
        Function<T, Object> fieldExtractor) {
        return new WindowedDistributionCheck<>(fieldName, fieldExtractor, 10, null, null);
    }
}
