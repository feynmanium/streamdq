package com.streamdq.profiling;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.function.Function;

/**
 * Profiles numeric fields in a streaming context, computing statistics over windows.
 * Similar to Deequ's profiling but adapted for Flink streams.
 *
 * @param <T> The type of data being profiled
 * @param <W> The type of window
 */
public class NumericProfiler<T, W extends Window> {
    private final String fieldName;
    private final Function<T, Number> fieldExtractor;

    public NumericProfiler(String fieldName, Function<T, Number> fieldExtractor) {
        this.fieldName = fieldName;
        this.fieldExtractor = fieldExtractor;
    }

    /**
     * Apply profiling to a windowed stream.
     */
    public DataStream<DataProfile> profile(WindowedStream<T, ?, W> windowedStream) {
        return windowedStream.aggregate(new NumericAggregator());
    }

    private class NumericAggregator implements AggregateFunction<T, NumericAccumulator, DataProfile> {
        private static final long serialVersionUID = 1L;

        @Override
        public NumericAccumulator createAccumulator() {
            return new NumericAccumulator();
        }

        @Override
        public NumericAccumulator add(T value, NumericAccumulator accumulator) {
            accumulator.count++;
            Number fieldValue = fieldExtractor.apply(value);

            if (fieldValue == null) {
                accumulator.nullCount++;
            } else {
                double doubleValue = fieldValue.doubleValue();
                accumulator.sum += doubleValue;
                accumulator.sumOfSquares += doubleValue * doubleValue;
                accumulator.min = Math.min(accumulator.min, doubleValue);
                accumulator.max = Math.max(accumulator.max, doubleValue);
            }

            return accumulator;
        }

        @Override
        public DataProfile getResult(NumericAccumulator accumulator) {
            long nonNullCount = accumulator.count - accumulator.nullCount;
            double mean = nonNullCount > 0 ? accumulator.sum / nonNullCount : 0.0;

            // Calculate standard deviation
            double variance = 0.0;
            if (nonNullCount > 1) {
                variance = (accumulator.sumOfSquares - (accumulator.sum * accumulator.sum / nonNullCount))
                    / (nonNullCount - 1);
            }
            double stdDev = Math.sqrt(Math.max(0, variance));

            return DataProfile.builder(fieldName)
                .totalRecords(accumulator.count)
                .nullCount(accumulator.nullCount)
                .min(accumulator.min == Double.MAX_VALUE ? null : accumulator.min)
                .max(accumulator.max == Double.MIN_VALUE ? null : accumulator.max)
                .mean(mean)
                .stdDev(stdDev)
                .build();
        }

        @Override
        public NumericAccumulator merge(NumericAccumulator a, NumericAccumulator b) {
            a.count += b.count;
            a.nullCount += b.nullCount;
            a.sum += b.sum;
            a.sumOfSquares += b.sumOfSquares;
            a.min = Math.min(a.min, b.min);
            a.max = Math.max(a.max, b.max);
            return a;
        }
    }

    private static class NumericAccumulator {
        long count = 0;
        long nullCount = 0;
        double sum = 0.0;
        double sumOfSquares = 0.0;
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
    }

    public static <T, W extends Window> NumericProfiler<T, W> on(
        String fieldName,
        Function<T, Number> fieldExtractor) {
        return new NumericProfiler<>(fieldName, fieldExtractor);
    }
}
