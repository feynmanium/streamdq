package com.streamdq.window;

import com.streamdq.api.CheckResult;
import com.streamdq.api.CheckStatus;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.function.Function;

/**
 * Windowed string length check - validates string length distribution within windows.
 *
 * Critical for text data quality:
 * - Detect truncation issues (max length suddenly at field limit)
 * - Monitor for empty/whitespace strings
 * - Validate length constraints
 * - Detect encoding issues (unexpected length distributions)
 *
 * Example use cases:
 * - Email addresses suddenly all 255 chars (truncated)
 * - Product descriptions all empty
 * - IDs not matching expected length
 *
 * @param <T> The type of data being checked
 * @param <W> The type of window
 */
public class WindowedStringLengthCheck<T, W extends Window> {
    private final String fieldName;
    private final Function<T, String> fieldExtractor;
    private final Integer minLength;
    private final Integer maxLength;
    private final Double violationThreshold; // Max acceptable violation rate

    public WindowedStringLengthCheck(String fieldName,
                                    Function<T, String> fieldExtractor,
                                    Integer minLength,
                                    Integer maxLength,
                                    double violationThreshold) {
        this.fieldName = fieldName;
        this.fieldExtractor = fieldExtractor;
        this.minLength = minLength;
        this.maxLength = maxLength;
        this.violationThreshold = violationThreshold;
    }

    /**
     * Apply string length check to a windowed stream.
     */
    public DataStream<CheckResult> apply(WindowedStream<T, ?, W> windowedStream) {
        return windowedStream.aggregate(new StringLengthAggregator());
    }

    private class StringLengthAggregator
        implements AggregateFunction<T, StringLengthAccumulator, CheckResult> {
        private static final long serialVersionUID = 1L;

        @Override
        public StringLengthAccumulator createAccumulator() {
            return new StringLengthAccumulator();
        }

        @Override
        public StringLengthAccumulator add(T value, StringLengthAccumulator acc) {
            acc.total++;
            String fieldValue = fieldExtractor.apply(value);

            if (fieldValue == null) {
                acc.nullCount++;
                return acc;
            }

            int length = fieldValue.length();
            acc.totalLength += length;
            acc.minLength = Math.min(acc.minLength, length);
            acc.maxLength = Math.max(acc.maxLength, length);

            // Check violations
            boolean tooShort = minLength != null && length < minLength;
            boolean tooLong = maxLength != null && length > maxLength;

            if (tooShort || tooLong) {
                acc.violations++;
            }

            // Track empty/whitespace
            if (length == 0) {
                acc.emptyCount++;
            } else if (fieldValue.trim().isEmpty()) {
                acc.whitespaceCount++;
            }

            return acc;
        }

        @Override
        public CheckResult getResult(StringLengthAccumulator acc) {
            long nonNullCount = acc.total - acc.nullCount;
            double violationRate = nonNullCount > 0
                ? (double) acc.violations / nonNullCount
                : 0.0;

            double avgLength = nonNullCount > 0
                ? (double) acc.totalLength / nonNullCount
                : 0.0;

            CheckResult.Builder builder = CheckResult.builder("WindowedStringLength_" + fieldName)
                .metricValue(1.0 - violationRate)
                .addMetadata("field", fieldName)
                .addMetadata("total_records", acc.total)
                .addMetadata("null_count", acc.nullCount)
                .addMetadata("min_length", acc.minLength != Integer.MAX_VALUE ? acc.minLength : 0)
                .addMetadata("max_length", acc.maxLength)
                .addMetadata("avg_length", avgLength)
                .addMetadata("empty_count", acc.emptyCount)
                .addMetadata("whitespace_count", acc.whitespaceCount)
                .addMetadata("violations", acc.violations)
                .addMetadata("violation_rate", violationRate);

            if (minLength != null) {
                builder.addMetadata("expected_min_length", minLength);
            }
            if (maxLength != null) {
                builder.addMetadata("expected_max_length", maxLength);
            }

            if (violationRate <= violationThreshold) {
                builder.status(CheckStatus.SUCCESS)
                    .description(String.format(
                        "String length violations %.2f%% within threshold %.2f%% (avg len: %.1f)",
                        violationRate * 100, violationThreshold * 100, avgLength));
            } else {
                builder.status(CheckStatus.ERROR)
                    .description(String.format(
                        "String length violations %.2f%% exceed threshold %.2f%% (%d violations)",
                        violationRate * 100, violationThreshold * 100, acc.violations));
            }

            return builder.build();
        }

        @Override
        public StringLengthAccumulator merge(StringLengthAccumulator a, StringLengthAccumulator b) {
            a.total += b.total;
            a.nullCount += b.nullCount;
            a.violations += b.violations;
            a.totalLength += b.totalLength;
            a.emptyCount += b.emptyCount;
            a.whitespaceCount += b.whitespaceCount;
            a.minLength = Math.min(a.minLength, b.minLength);
            a.maxLength = Math.max(a.maxLength, b.maxLength);
            return a;
        }
    }

    private static class StringLengthAccumulator {
        long total = 0;
        long nullCount = 0;
        long violations = 0;
        long totalLength = 0;
        long emptyCount = 0;
        long whitespaceCount = 0;
        int minLength = Integer.MAX_VALUE;
        int maxLength = 0;
    }

    public static <T, W extends Window> WindowedStringLengthCheck<T, W> between(
        String fieldName,
        Function<T, String> fieldExtractor,
        int minLength,
        int maxLength,
        double violationThreshold) {
        return new WindowedStringLengthCheck<>(fieldName, fieldExtractor,
            minLength, maxLength, violationThreshold);
    }

    public static <T, W extends Window> WindowedStringLengthCheck<T, W> minLength(
        String fieldName,
        Function<T, String> fieldExtractor,
        int minLength,
        double violationThreshold) {
        return new WindowedStringLengthCheck<>(fieldName, fieldExtractor,
            minLength, null, violationThreshold);
    }

    public static <T, W extends Window> WindowedStringLengthCheck<T, W> maxLength(
        String fieldName,
        Function<T, String> fieldExtractor,
        int maxLength,
        double violationThreshold) {
        return new WindowedStringLengthCheck<>(fieldName, fieldExtractor,
            null, maxLength, violationThreshold);
    }

    /**
     * Detect non-empty strings (length > 0).
     */
    public static <T, W extends Window> WindowedStringLengthCheck<T, W> notEmpty(
        String fieldName,
        Function<T, String> fieldExtractor,
        double violationThreshold) {
        return new WindowedStringLengthCheck<>(fieldName, fieldExtractor,
            1, null, violationThreshold);
    }
}
