package com.streamdq.alerting;

import com.streamdq.api.CheckResult;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a data quality alert that should be sent to external systems.
 *
 * Critical for streaming DQ:
 * - Batch alerts (next day) are too late
 * - Need immediate notification of violations
 * - Enable real-time response to data issues
 */
public class Alert implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String alertId;
    private final AlertSeverity severity;
    private final String title;
    private final String message;
    private final CheckResult triggeringCheck;
    private final Instant timestamp;
    private final Map<String, Object> context;

    private Alert(Builder builder) {
        this.alertId = builder.alertId;
        this.severity = builder.severity;
        this.title = builder.title;
        this.message = builder.message;
        this.triggeringCheck = builder.triggeringCheck;
        this.timestamp = builder.timestamp != null ? builder.timestamp : Instant.now();
        this.context = new HashMap<>(builder.context);
    }

    public String getAlertId() {
        return alertId;
    }

    public AlertSeverity getSeverity() {
        return severity;
    }

    public String getTitle() {
        return title;
    }

    public String getMessage() {
        return message;
    }

    public CheckResult getTriggeringCheck() {
        return triggeringCheck;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public Map<String, Object> getContext() {
        return new HashMap<>(context);
    }

    public static Builder builder(String alertId) {
        return new Builder(alertId);
    }

    public static class Builder {
        private final String alertId;
        private AlertSeverity severity = AlertSeverity.WARNING;
        private String title;
        private String message;
        private CheckResult triggeringCheck;
        private Instant timestamp;
        private Map<String, Object> context = new HashMap<>();

        private Builder(String alertId) {
            this.alertId = alertId;
        }

        public Builder severity(AlertSeverity severity) {
            this.severity = severity;
            return this;
        }

        public Builder title(String title) {
            this.title = title;
            return this;
        }

        public Builder message(String message) {
            this.message = message;
            return this;
        }

        public Builder triggeringCheck(CheckResult check) {
            this.triggeringCheck = check;
            return this;
        }

        public Builder timestamp(Instant timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder addContext(String key, Object value) {
            this.context.put(key, value);
            return this;
        }

        public Builder context(Map<String, Object> context) {
            this.context = new HashMap<>(context);
            return this;
        }

        public Alert build() {
            if (title == null) {
                title = "Data Quality Alert: " + alertId;
            }
            if (message == null && triggeringCheck != null) {
                message = triggeringCheck.getDescription();
            }
            return new Alert(this);
        }
    }

    @Override
    public String toString() {
        return String.format("[%s] %s: %s (check: %s, severity: %s, time: %s)",
            alertId, title, message,
            triggeringCheck != null ? triggeringCheck.getCheckName() : "N/A",
            severity, timestamp);
    }
}
