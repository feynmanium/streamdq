package com.streamdq.checks;

import com.streamdq.api.Check;
import com.streamdq.api.CheckResult;
import com.streamdq.api.CheckStatus;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.function.Function;

/**
 * Checks that a numeric field falls within a specified range.
 * Similar to Deequ's min/max constraints.
 *
 * @param <T> The type of data being checked
 */
public class RangeCheck<T> implements Check<T> {
    private static final long serialVersionUID = 1L;

    private final String fieldName;
    private final Function<T, Number> fieldExtractor;
    private final Double min;
    private final Double max;

    public RangeCheck(String fieldName, Function<T, Number> fieldExtractor, Double min, Double max) {
        this.fieldName = fieldName;
        this.fieldExtractor = fieldExtractor;
        this.min = min;
        this.max = max;
    }

    @Override
    public String getName() {
        return "Range_" + fieldName;
    }

    @Override
    public String getDescription() {
        return String.format("Checks that field '%s' is between %.2f and %.2f", fieldName, min, max);
    }

    @Override
    public MapFunction<T, CheckResult> toMapFunction() {
        return this::validate;
    }

    @Override
    public CheckResult validate(T element) {
        Number value = fieldExtractor.apply(element);

        CheckResult.Builder builder = CheckResult.builder(getName())
            .addMetadata("field", fieldName)
            .addMetadata("min", min)
            .addMetadata("max", max);

        if (value == null) {
            return builder
                .status(CheckStatus.ERROR)
                .metricValue(0.0)
                .description("Field '" + fieldName + "' is null")
                .build();
        }

        double doubleValue = value.doubleValue();
        boolean inRange = doubleValue >= min && doubleValue <= max;

        if (inRange) {
            builder.status(CheckStatus.SUCCESS)
                .metricValue(1.0)
                .description("Field '" + fieldName + "' is in range");
        } else {
            builder.status(CheckStatus.ERROR)
                .metricValue(0.0)
                .description(String.format("Field '%s' is out of range. Value: %.2f", fieldName, doubleValue));
        }

        return builder.build();
    }

    public static <T> RangeCheck<T> on(String fieldName, Function<T, Number> fieldExtractor,
                                        Double min, Double max) {
        return new RangeCheck<>(fieldName, fieldExtractor, min, max);
    }

    public static <T> RangeCheck<T> greaterThan(String fieldName, Function<T, Number> fieldExtractor,
                                                 Double threshold) {
        return new RangeCheck<>(fieldName, fieldExtractor, threshold, Double.MAX_VALUE);
    }

    public static <T> RangeCheck<T> lessThan(String fieldName, Function<T, Number> fieldExtractor,
                                             Double threshold) {
        return new RangeCheck<>(fieldName, fieldExtractor, Double.MIN_VALUE, threshold);
    }

    public static <T> RangeCheck<T> positive(String fieldName, Function<T, Number> fieldExtractor) {
        return new RangeCheck<>(fieldName, fieldExtractor, 0.0, Double.MAX_VALUE);
    }

    public static <T> RangeCheck<T> negative(String fieldName, Function<T, Number> fieldExtractor) {
        return new RangeCheck<>(fieldName, fieldExtractor, Double.MIN_VALUE, 0.0);
    }
}
