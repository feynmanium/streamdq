package com.streamdq.checks;

import com.streamdq.api.Check;
import com.streamdq.api.CheckResult;
import com.streamdq.api.CheckStatus;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * Checks that a field matches a specified regex pattern.
 * Similar to Deequ's pattern constraint.
 *
 * @param <T> The type of data being checked
 */
public class PatternCheck<T> implements Check<T> {
    private static final long serialVersionUID = 1L;

    private final String fieldName;
    private final Function<T, String> fieldExtractor;
    private final Pattern pattern;
    private final String patternString;

    public PatternCheck(String fieldName, Function<T, String> fieldExtractor, String regex) {
        this.fieldName = fieldName;
        this.fieldExtractor = fieldExtractor;
        this.patternString = regex;
        this.pattern = Pattern.compile(regex);
    }

    @Override
    public String getName() {
        return "Pattern_" + fieldName;
    }

    @Override
    public String getDescription() {
        return String.format("Checks that field '%s' matches pattern: %s", fieldName, patternString);
    }

    @Override
    public MapFunction<T, CheckResult> toMapFunction() {
        return this::validate;
    }

    @Override
    public CheckResult validate(T element) {
        String value = fieldExtractor.apply(element);

        CheckResult.Builder builder = CheckResult.builder(getName())
            .addMetadata("field", fieldName)
            .addMetadata("pattern", patternString);

        if (value == null) {
            return builder
                .status(CheckStatus.ERROR)
                .metricValue(0.0)
                .description("Field '" + fieldName + "' is null")
                .build();
        }

        boolean matches = pattern.matcher(value).matches();

        if (matches) {
            builder.status(CheckStatus.SUCCESS)
                .metricValue(1.0)
                .description("Field '" + fieldName + "' matches pattern");
        } else {
            builder.status(CheckStatus.ERROR)
                .metricValue(0.0)
                .description("Field '" + fieldName + "' does not match pattern. Value: " + value);
        }

        return builder.build();
    }

    public static <T> PatternCheck<T> on(String fieldName, Function<T, String> fieldExtractor, String regex) {
        return new PatternCheck<>(fieldName, fieldExtractor, regex);
    }

    // Common patterns
    public static <T> PatternCheck<T> email(String fieldName, Function<T, String> fieldExtractor) {
        return new PatternCheck<>(fieldName, fieldExtractor,
            "^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$");
    }

    public static <T> PatternCheck<T> url(String fieldName, Function<T, String> fieldExtractor) {
        return new PatternCheck<>(fieldName, fieldExtractor,
            "^(https?|ftp)://[^\\s/$.?#].[^\\s]*$");
    }

    public static <T> PatternCheck<T> phone(String fieldName, Function<T, String> fieldExtractor) {
        return new PatternCheck<>(fieldName, fieldExtractor,
            "^\\+?[1-9]\\d{1,14}$"); // E.164 format
    }

    public static <T> PatternCheck<T> uuid(String fieldName, Function<T, String> fieldExtractor) {
        return new PatternCheck<>(fieldName, fieldExtractor,
            "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");
    }
}
