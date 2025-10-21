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
 * Windowed cardinality check - tracks distinct count of values within windows.
 *
 * Cardinality is a fundamental DQ dimension in streaming:
 * - Detect unexpected value explosions (e.g., user IDs suddenly 10x higher)
 * - Validate dimension tables (should have stable cardinality)
 * - Monitor for data quality issues (new invalid values appearing)
 *
 * For very high cardinality (millions of distinct values), consider using
 * approximate algorithms like HyperLogLog to reduce memory usage.
 *
 * @param <T> The type of data being checked
 * @param <W> The type of window
 */
public class WindowedCardinalityCheck<T, W extends Window> {
    private final String fieldName;
    private final Function<T, Object> fieldExtractor;
    private final Long minCardinality;
    private final Long maxCardinality;

    public WindowedCardinalityCheck(String fieldName,
                                   Function<T, Object> fieldExtractor,
                                   Long minCardinality,
                                   Long maxCardinality) {
        this.fieldName = fieldName;
        this.fieldExtractor = fieldExtractor;
        this.minCardinality = minCardinality;
        this.maxCardinality = maxCardinality;
    }

    /**
     * Apply cardinality check to a windowed stream.
     */
    public DataStream<CheckResult> apply(WindowedStream<T, ?, W> windowedStream) {
        return windowedStream.aggregate(new CardinalityAggregator());
    }

    private class CardinalityAggregator implements AggregateFunction<T, CardinalityAccumulator, CheckResult> {
        private static final long serialVersionUID = 1L;

        @Override
        public CardinalityAccumulator createAccumulator() {
            return new CardinalityAccumulator();
        }

        @Override
        public CardinalityAccumulator add(T value, CardinalityAccumulator acc) {
            acc.total++;
            Object fieldValue = fieldExtractor.apply(value);

            if (fieldValue != null) {
                acc.distinctValues.add(fieldValue);
            } else {
                acc.nullCount++;
            }

            return acc;
        }

        @Override
        public CheckResult getResult(CardinalityAccumulator acc) {
            long cardinality = acc.distinctValues.size();

            CheckResult.Builder builder = CheckResult.builder("WindowedCardinality_" + fieldName)
                .metricValue((double) cardinality)
                .addMetadata("field", fieldName)
                .addMetadata("cardinality", cardinality)
                .addMetadata("total_records", acc.total)
                .addMetadata("null_count", acc.nullCount);

            if (minCardinality != null) {
                builder.addMetadata("min_cardinality", minCardinality);
            }
            if (maxCardinality != null) {
                builder.addMetadata("max_cardinality", maxCardinality);
            }

            // Check bounds
            boolean inRange = true;
            String reason = "";

            if (minCardinality != null && cardinality < minCardinality) {
                inRange = false;
                reason = String.format("Cardinality %d below minimum %d", cardinality, minCardinality);
            } else if (maxCardinality != null && cardinality > maxCardinality) {
                inRange = false;
                reason = String.format("Cardinality %d exceeds maximum %d", cardinality, maxCardinality);
            }

            if (inRange) {
                builder.status(CheckStatus.SUCCESS)
                    .description(String.format("Cardinality %d within expected range", cardinality));
            } else {
                builder.status(CheckStatus.ERROR)
                    .description(reason);
            }

            return builder.build();
        }

        @Override
        public CardinalityAccumulator merge(CardinalityAccumulator a, CardinalityAccumulator b) {
            a.total += b.total;
            a.nullCount += b.nullCount;
            a.distinctValues.addAll(b.distinctValues);
            return a;
        }
    }

    private static class CardinalityAccumulator {
        long total = 0;
        long nullCount = 0;
        Set<Object> distinctValues = new HashSet<>();
    }

    public static <T, W extends Window> WindowedCardinalityCheck<T, W> between(
        String fieldName,
        Function<T, Object> fieldExtractor,
        long minCardinality,
        long maxCardinality) {
        return new WindowedCardinalityCheck<>(fieldName, fieldExtractor, minCardinality, maxCardinality);
    }

    public static <T, W extends Window> WindowedCardinalityCheck<T, W> atLeast(
        String fieldName,
        Function<T, Object> fieldExtractor,
        long minCardinality) {
        return new WindowedCardinalityCheck<>(fieldName, fieldExtractor, minCardinality, null);
    }

    public static <T, W extends Window> WindowedCardinalityCheck<T, W> atMost(
        String fieldName,
        Function<T, Object> fieldExtractor,
        long maxCardinality) {
        return new WindowedCardinalityCheck<>(fieldName, fieldExtractor, null, maxCardinality);
    }

    /**
     * Just track cardinality without bounds checking.
     */
    public static <T, W extends Window> WindowedCardinalityCheck<T, W> track(
        String fieldName,
        Function<T, Object> fieldExtractor) {
        return new WindowedCardinalityCheck<>(fieldName, fieldExtractor, null, null);
    }
}
