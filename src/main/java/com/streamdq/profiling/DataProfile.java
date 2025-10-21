package com.streamdq.profiling;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Statistical profile of data within a window.
 * Similar to Deequ's ColumnProfile but for streaming data.
 */
public class DataProfile implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String fieldName;
    private final long totalRecords;
    private final long nullCount;
    private final long distinctCount;
    private final Double min;
    private final Double max;
    private final Double mean;
    private final Double stdDev;
    private final Instant windowStart;
    private final Instant windowEnd;
    private final Map<String, Object> additionalMetrics;

    private DataProfile(Builder builder) {
        this.fieldName = builder.fieldName;
        this.totalRecords = builder.totalRecords;
        this.nullCount = builder.nullCount;
        this.distinctCount = builder.distinctCount;
        this.min = builder.min;
        this.max = builder.max;
        this.mean = builder.mean;
        this.stdDev = builder.stdDev;
        this.windowStart = builder.windowStart;
        this.windowEnd = builder.windowEnd;
        this.additionalMetrics = new HashMap<>(builder.additionalMetrics);
    }

    public String getFieldName() {
        return fieldName;
    }

    public long getTotalRecords() {
        return totalRecords;
    }

    public long getNullCount() {
        return nullCount;
    }

    public long getDistinctCount() {
        return distinctCount;
    }

    public Double getMin() {
        return min;
    }

    public Double getMax() {
        return max;
    }

    public Double getMean() {
        return mean;
    }

    public Double getStdDev() {
        return stdDev;
    }

    public double getCompleteness() {
        return totalRecords > 0 ? 1.0 - ((double) nullCount / totalRecords) : 1.0;
    }

    public Instant getWindowStart() {
        return windowStart;
    }

    public Instant getWindowEnd() {
        return windowEnd;
    }

    public Map<String, Object> getAdditionalMetrics() {
        return new HashMap<>(additionalMetrics);
    }

    public static Builder builder(String fieldName) {
        return new Builder(fieldName);
    }

    public static class Builder {
        private final String fieldName;
        private long totalRecords;
        private long nullCount;
        private long distinctCount;
        private Double min;
        private Double max;
        private Double mean;
        private Double stdDev;
        private Instant windowStart;
        private Instant windowEnd;
        private Map<String, Object> additionalMetrics = new HashMap<>();

        private Builder(String fieldName) {
            this.fieldName = fieldName;
        }

        public Builder totalRecords(long totalRecords) {
            this.totalRecords = totalRecords;
            return this;
        }

        public Builder nullCount(long nullCount) {
            this.nullCount = nullCount;
            return this;
        }

        public Builder distinctCount(long distinctCount) {
            this.distinctCount = distinctCount;
            return this;
        }

        public Builder min(Double min) {
            this.min = min;
            return this;
        }

        public Builder max(Double max) {
            this.max = max;
            return this;
        }

        public Builder mean(Double mean) {
            this.mean = mean;
            return this;
        }

        public Builder stdDev(Double stdDev) {
            this.stdDev = stdDev;
            return this;
        }

        public Builder windowStart(Instant windowStart) {
            this.windowStart = windowStart;
            return this;
        }

        public Builder windowEnd(Instant windowEnd) {
            this.windowEnd = windowEnd;
            return this;
        }

        public Builder addMetric(String key, Object value) {
            this.additionalMetrics.put(key, value);
            return this;
        }

        public DataProfile build() {
            return new DataProfile(this);
        }
    }

    @Override
    public String toString() {
        return String.format(
            "DataProfile{field='%s', records=%d, nulls=%d, distinct=%d, " +
            "min=%.2f, max=%.2f, mean=%.2f, stdDev=%.2f, completeness=%.2f%%}",
            fieldName, totalRecords, nullCount, distinctCount,
            min, max, mean, stdDev, getCompleteness() * 100
        );
    }
}
