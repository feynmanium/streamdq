package com.streamdq.checks;

import com.streamdq.api.Check;
import com.streamdq.api.CheckResult;
import com.streamdq.api.CheckStatus;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.function.Function;

/**
 * Checks for completeness (non-null values) in a data stream.
 * Similar to Deequ's completeness constraint.
 *
 * @param <T> The type of data being checked
 */
public class CompletenessCheck<T> implements Check<T> {
    private static final long serialVersionUID = 1L;

    private final String fieldName;
    private final Function<T, Object> fieldExtractor;
    private final double threshold; // Minimum acceptable non-null rate (0.0 to 1.0)

    public CompletenessCheck(String fieldName, Function<T, Object> fieldExtractor) {
        this(fieldName, fieldExtractor, 1.0);
    }

    public CompletenessCheck(String fieldName, Function<T, Object> fieldExtractor, double threshold) {
        this.fieldName = fieldName;
        this.fieldExtractor = fieldExtractor;
        this.threshold = threshold;
    }

    @Override
    public String getName() {
        return "Completeness_" + fieldName;
    }

    @Override
    public String getDescription() {
        return String.format("Checks that field '%s' has no null values (threshold: %.2f%%)",
            fieldName, threshold * 100);
    }

    @Override
    public MapFunction<T, CheckResult> toMapFunction() {
        return this::validate;
    }

    @Override
    public CheckResult validate(T element) {
        Object value = fieldExtractor.apply(element);
        boolean isComplete = value != null;

        CheckResult.Builder builder = CheckResult.builder(getName())
            .metricValue(isComplete ? 1.0 : 0.0)
            .addMetadata("field", fieldName)
            .addMetadata("threshold", threshold);

        if (isComplete) {
            builder.status(CheckStatus.SUCCESS)
                .description("Field '" + fieldName + "' is complete");
        } else {
            builder.status(CheckStatus.ERROR)
                .description("Field '" + fieldName + "' is null");
        }

        return builder.build();
    }

    public static <T> CompletenessCheck<T> on(String fieldName, Function<T, Object> fieldExtractor) {
        return new CompletenessCheck<>(fieldName, fieldExtractor);
    }

    public static <T> CompletenessCheck<T> on(String fieldName, Function<T, Object> fieldExtractor, double threshold) {
        return new CompletenessCheck<>(fieldName, fieldExtractor, threshold);
    }
}
