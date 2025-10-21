package com.streamdq.window;

import com.streamdq.api.CheckResult;
import com.streamdq.api.CheckStatus;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.function.Function;

/**
 * Windowed pattern conformance check - tracks percentage of values matching a pattern.
 *
 * Unlike per-record PatternCheck (pass/fail), this aggregates conformance rates
 * over windows to detect degradation in data quality:
 * - Email format conformance dropping from 99% to 85%
 * - Phone number validation rate declining
 * - SKU format violations increasing
 *
 * This is critical for streaming DQ to detect gradual quality degradation
 * that per-record checks miss.
 *
 * @param <T> The type of data being checked
 * @param <W> The type of window
 */
public class WindowedPatternConformanceCheck<T, W extends Window> {
    private final String fieldName;
    private final Function<T, String> fieldExtractor;
    private final String pattern;
    private final double conformanceThreshold; // Min acceptable conformance rate

    public WindowedPatternConformanceCheck(String fieldName,
                                          Function<T, String> fieldExtractor,
                                          String pattern,
                                          double conformanceThreshold) {
        this.fieldName = fieldName;
        this.fieldExtractor = fieldExtractor;
        this.pattern = pattern;
        this.conformanceThreshold = conformanceThreshold;
    }

    /**
     * Apply pattern conformance check to a windowed stream.
     */
    public DataStream<CheckResult> apply(WindowedStream<T, ?, W> windowedStream) {
        return windowedStream.aggregate(new PatternConformanceAggregator());
    }

    private class PatternConformanceAggregator
        implements AggregateFunction<T, ConformanceAccumulator, CheckResult> {
        private static final long serialVersionUID = 1L;

        private final java.util.regex.Pattern compiledPattern = java.util.regex.Pattern.compile(pattern);

        @Override
        public ConformanceAccumulator createAccumulator() {
            return new ConformanceAccumulator();
        }

        @Override
        public ConformanceAccumulator add(T value, ConformanceAccumulator acc) {
            acc.total++;
            String fieldValue = fieldExtractor.apply(value);

            if (fieldValue == null) {
                acc.nullCount++;
            } else if (compiledPattern.matcher(fieldValue).matches()) {
                acc.conforming++;
            } else {
                acc.nonConforming++;
            }

            return acc;
        }

        @Override
        public CheckResult getResult(ConformanceAccumulator acc) {
            long nonNullCount = acc.total - acc.nullCount;
            double conformanceRate = nonNullCount > 0
                ? (double) acc.conforming / nonNullCount
                : 1.0;

            CheckResult.Builder builder = CheckResult.builder("WindowedPatternConformance_" + fieldName)
                .metricValue(conformanceRate)
                .addMetadata("field", fieldName)
                .addMetadata("pattern", pattern)
                .addMetadata("threshold", conformanceThreshold)
                .addMetadata("total_records", acc.total)
                .addMetadata("conforming", acc.conforming)
                .addMetadata("non_conforming", acc.nonConforming)
                .addMetadata("null_count", acc.nullCount);

            if (conformanceRate >= conformanceThreshold) {
                builder.status(CheckStatus.SUCCESS)
                    .description(String.format("Pattern conformance %.2f%% meets threshold %.2f%%",
                        conformanceRate * 100, conformanceThreshold * 100));
            } else {
                builder.status(CheckStatus.ERROR)
                    .description(String.format("Pattern conformance %.2f%% below threshold %.2f%% (%d violations)",
                        conformanceRate * 100, conformanceThreshold * 100, acc.nonConforming));
            }

            return builder.build();
        }

        @Override
        public ConformanceAccumulator merge(ConformanceAccumulator a, ConformanceAccumulator b) {
            a.total += b.total;
            a.conforming += b.conforming;
            a.nonConforming += b.nonConforming;
            a.nullCount += b.nullCount;
            return a;
        }
    }

    private static class ConformanceAccumulator {
        long total = 0;
        long conforming = 0;
        long nonConforming = 0;
        long nullCount = 0;
    }

    public static <T, W extends Window> WindowedPatternConformanceCheck<T, W> on(
        String fieldName,
        Function<T, String> fieldExtractor,
        String pattern,
        double conformanceThreshold) {
        return new WindowedPatternConformanceCheck<>(fieldName, fieldExtractor, pattern, conformanceThreshold);
    }

    // Common patterns
    public static <T, W extends Window> WindowedPatternConformanceCheck<T, W> email(
        String fieldName,
        Function<T, String> fieldExtractor,
        double conformanceThreshold) {
        return new WindowedPatternConformanceCheck<>(fieldName, fieldExtractor,
            "^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$", conformanceThreshold);
    }

    public static <T, W extends Window> WindowedPatternConformanceCheck<T, W> phone(
        String fieldName,
        Function<T, String> fieldExtractor,
        double conformanceThreshold) {
        return new WindowedPatternConformanceCheck<>(fieldName, fieldExtractor,
            "^\\+?[1-9]\\d{1,14}$", conformanceThreshold);
    }

    public static <T, W extends Window> WindowedPatternConformanceCheck<T, W> uuid(
        String fieldName,
        Function<T, String> fieldExtractor,
        double conformanceThreshold) {
        return new WindowedPatternConformanceCheck<>(fieldName, fieldExtractor,
            "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
            conformanceThreshold);
    }
}
