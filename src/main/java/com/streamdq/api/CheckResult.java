package com.streamdq.api;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Result of a data quality check execution.
 */
public class CheckResult implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String checkName;
    private final CheckStatus status;
    private final String description;
    private final Double metricValue;
    private final Instant timestamp;
    private final Map<String, Object> metadata;

    private CheckResult(Builder builder) {
        this.checkName = Objects.requireNonNull(builder.checkName, "checkName cannot be null");
        this.status = Objects.requireNonNull(builder.status, "status cannot be null");
        this.description = builder.description;
        this.metricValue = builder.metricValue;
        this.timestamp = builder.timestamp != null ? builder.timestamp : Instant.now();
        this.metadata = new HashMap<>(builder.metadata);
    }

    public String getCheckName() {
        return checkName;
    }

    public CheckStatus getStatus() {
        return status;
    }

    public String getDescription() {
        return description;
    }

    public Double getMetricValue() {
        return metricValue;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public Map<String, Object> getMetadata() {
        return new HashMap<>(metadata);
    }

    public boolean isSuccess() {
        return status == CheckStatus.SUCCESS;
    }

    public static Builder builder(String checkName) {
        return new Builder(checkName);
    }

    public static class Builder {
        private final String checkName;
        private CheckStatus status = CheckStatus.SUCCESS;
        private String description = "";
        private Double metricValue;
        private Instant timestamp;
        private Map<String, Object> metadata = new HashMap<>();

        private Builder(String checkName) {
            this.checkName = checkName;
        }

        public Builder status(CheckStatus status) {
            this.status = status;
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder metricValue(Double metricValue) {
            this.metricValue = metricValue;
            return this;
        }

        public Builder timestamp(Instant timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder addMetadata(String key, Object value) {
            this.metadata.put(key, value);
            return this;
        }

        public Builder metadata(Map<String, Object> metadata) {
            this.metadata = new HashMap<>(metadata);
            return this;
        }

        public CheckResult build() {
            return new CheckResult(this);
        }
    }

    @Override
    public String toString() {
        return String.format("CheckResult{name='%s', status=%s, metric=%.4f, description='%s', timestamp=%s}",
                checkName, status, metricValue, description, timestamp);
    }
}
