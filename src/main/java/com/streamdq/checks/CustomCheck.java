package com.streamdq.checks;

import com.streamdq.api.Check;
import com.streamdq.api.CheckResult;
import com.streamdq.api.CheckStatus;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.function.Function;

/**
 * Custom check that allows users to define their own validation logic.
 * This provides maximum flexibility for domain-specific validations.
 *
 * @param <T> The type of data being checked
 */
public class CustomCheck<T> implements Check<T> {
    private static final long serialVersionUID = 1L;

    private final String name;
    private final String description;
    private final Function<T, Boolean> validationFunction;
    private final Function<T, String> errorMessageProvider;

    public CustomCheck(String name,
                      String description,
                      Function<T, Boolean> validationFunction,
                      Function<T, String> errorMessageProvider) {
        this.name = name;
        this.description = description;
        this.validationFunction = validationFunction;
        this.errorMessageProvider = errorMessageProvider;
    }

    public CustomCheck(String name,
                      String description,
                      Function<T, Boolean> validationFunction) {
        this(name, description, validationFunction,
            element -> "Custom validation failed for: " + element);
    }

    @Override
    public String getName() {
        return "Custom_" + name;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public MapFunction<T, CheckResult> toMapFunction() {
        return this::validate;
    }

    @Override
    public CheckResult validate(T element) {
        try {
            boolean isValid = validationFunction.apply(element);

            if (isValid) {
                return CheckResult.builder(getName())
                    .status(CheckStatus.SUCCESS)
                    .metricValue(1.0)
                    .description("Custom check passed")
                    .build();
            } else {
                return CheckResult.builder(getName())
                    .status(CheckStatus.ERROR)
                    .metricValue(0.0)
                    .description(errorMessageProvider.apply(element))
                    .build();
            }
        } catch (Exception e) {
            return CheckResult.builder(getName())
                .status(CheckStatus.ERROR)
                .metricValue(0.0)
                .description("Exception during validation: " + e.getMessage())
                .addMetadata("exception", e.getClass().getName())
                .build();
        }
    }

    public static <T> CustomCheck<T> of(String name,
                                        String description,
                                        Function<T, Boolean> validationFunction) {
        return new CustomCheck<>(name, description, validationFunction);
    }

    public static <T> CustomCheck<T> of(String name,
                                        String description,
                                        Function<T, Boolean> validationFunction,
                                        Function<T, String> errorMessageProvider) {
        return new CustomCheck<>(name, description, validationFunction, errorMessageProvider);
    }
}
