package com.streamdq.api;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Main entry point for StreamDQ. Applies data quality checks to Flink DataStreams.
 * Similar to Deequ's VerificationSuite but designed for streaming.
 *
 * @param <T> The type of data being verified
 */
public class DataQualityVerifier<T> implements Serializable {
    private static final long serialVersionUID = 1L;

    private final List<Check<T>> checks;
    private boolean failOnError = false;

    private DataQualityVerifier(Builder<T> builder) {
        this.checks = new ArrayList<>(builder.checks);
        this.failOnError = builder.failOnError;
    }

    /**
     * Apply all checks to a DataStream and return a stream of CheckResults.
     */
    public DataStream<CheckResult> verify(DataStream<T> inputStream) {
        DataStream<CheckResult> results = null;

        for (Check<T> check : checks) {
            DataStream<CheckResult> checkStream = inputStream.map(check.toMapFunction())
                .name(check.getName());

            if (results == null) {
                results = checkStream;
            } else {
                results = results.union(checkStream);
            }
        }

        return results;
    }

    /**
     * Apply checks and use side outputs to separate SUCCESS and ERROR results.
     */
    public VerificationResult<T> verifyWithSideOutputs(DataStream<T> inputStream) {
        OutputTag<CheckResult> errorTag = new OutputTag<CheckResult>("dq-errors") {};
        OutputTag<CheckResult> successTag = new OutputTag<CheckResult>("dq-success") {};

        // Process each check
        SingleOutputStreamOperator<CheckResult> mainStream = null;

        for (Check<T> check : checks) {
            SingleOutputStreamOperator<CheckResult> checkStream = inputStream
                .map(check.toMapFunction())
                .name(check.getName());

            if (mainStream == null) {
                mainStream = checkStream;
            } else {
                mainStream = mainStream.union(checkStream);
            }
        }

        // Split into success and error streams
        SingleOutputStreamOperator<CheckResult> processedStream = mainStream
            .process(new CheckResultSplitter(errorTag, successTag))
            .name("DQ-Splitter");

        return new VerificationResult<>(
            processedStream,
            processedStream.getSideOutput(successTag),
            processedStream.getSideOutput(errorTag)
        );
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    public static class Builder<T> {
        private final List<Check<T>> checks = new ArrayList<>();
        private boolean failOnError = false;

        public Builder<T> addCheck(Check<T> check) {
            this.checks.add(check);
            return this;
        }

        public Builder<T> addChecks(List<Check<T>> checks) {
            this.checks.addAll(checks);
            return this;
        }

        public Builder<T> failOnError(boolean failOnError) {
            this.failOnError = failOnError;
            return this;
        }

        public DataQualityVerifier<T> build() {
            if (checks.isEmpty()) {
                throw new IllegalStateException("At least one check must be added");
            }
            return new DataQualityVerifier<>(this);
        }
    }

    /**
     * Result container that holds success, error, and all result streams.
     */
    public static class VerificationResult<T> {
        private final DataStream<CheckResult> allResults;
        private final DataStream<CheckResult> successResults;
        private final DataStream<CheckResult> errorResults;

        public VerificationResult(DataStream<CheckResult> allResults,
                                 DataStream<CheckResult> successResults,
                                 DataStream<CheckResult> errorResults) {
            this.allResults = allResults;
            this.successResults = successResults;
            this.errorResults = errorResults;
        }

        public DataStream<CheckResult> getAllResults() {
            return allResults;
        }

        public DataStream<CheckResult> getSuccessResults() {
            return successResults;
        }

        public DataStream<CheckResult> getErrorResults() {
            return errorResults;
        }
    }
}
