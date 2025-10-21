package com.streamdq.api;

import java.io.Serializable;
import java.util.function.Function;

/**
 * A constraint that can be evaluated against a value to determine if it meets expectations.
 *
 * @param <T> The type of value being constrained
 */
public class Constraint<T> implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String name;
    private final Function<T, Boolean> predicate;
    private final String errorMessage;

    public Constraint(String name, Function<T, Boolean> predicate, String errorMessage) {
        this.name = name;
        this.predicate = predicate;
        this.errorMessage = errorMessage;
    }

    public String getName() {
        return name;
    }

    public boolean test(T value) {
        try {
            return predicate.apply(value);
        } catch (Exception e) {
            return false;
        }
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public static <T> Constraint<T> isNotNull(String fieldName) {
        return new Constraint<>(
            fieldName + "_not_null",
            value -> value != null,
            fieldName + " must not be null"
        );
    }

    public static Constraint<String> isNotEmpty(String fieldName) {
        return new Constraint<>(
            fieldName + "_not_empty",
            value -> value != null && !value.trim().isEmpty(),
            fieldName + " must not be empty"
        );
    }

    public static Constraint<String> matchesPattern(String fieldName, String regex) {
        return new Constraint<>(
            fieldName + "_pattern",
            value -> value != null && value.matches(regex),
            fieldName + " must match pattern: " + regex
        );
    }

    public static <T extends Comparable<T>> Constraint<T> isInRange(String fieldName, T min, T max) {
        return new Constraint<>(
            fieldName + "_range",
            value -> value != null && value.compareTo(min) >= 0 && value.compareTo(max) <= 0,
            fieldName + " must be between " + min + " and " + max
        );
    }

    public static Constraint<Number> isGreaterThan(String fieldName, double threshold) {
        return new Constraint<>(
            fieldName + "_greater_than",
            value -> value != null && value.doubleValue() > threshold,
            fieldName + " must be greater than " + threshold
        );
    }

    public static Constraint<Number> isLessThan(String fieldName, double threshold) {
        return new Constraint<>(
            fieldName + "_less_than",
            value -> value != null && value.doubleValue() < threshold,
            fieldName + " must be less than " + threshold
        );
    }
}
