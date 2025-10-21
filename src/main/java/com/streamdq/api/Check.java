package com.streamdq.api;

import org.apache.flink.api.common.functions.MapFunction;

import java.io.Serializable;

/**
 * Base interface for all data quality checks.
 *
 * @param <T> The type of data being checked
 */
public interface Check<T> extends Serializable {

    /**
     * Get the name of this check.
     */
    String getName();

    /**
     * Get a description of what this check does.
     */
    String getDescription();

    /**
     * Convert this check into a Flink MapFunction that can be applied to a stream.
     */
    MapFunction<T, CheckResult> toMapFunction();

    /**
     * Validates a single element and returns a check result.
     * This is used internally by toMapFunction().
     */
    CheckResult validate(T element);
}
