package com.streamdq.alerting;

import com.streamdq.api.CheckResult;
import com.streamdq.api.CheckStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Process function that converts check results to alerts and sends them.
 *
 * Critical for streaming DQ:
 * - Immediate notification of data quality issues
 * - Real-time incident response
 * - Integration with operational tools
 */
public class AlertingFunction extends ProcessFunction<CheckResult, Alert> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(AlertingFunction.class);

    private final List<AlertSink> alertSinks;
    private final AlertSeverity minSeverity;
    private final boolean alertOnSuccess;

    public AlertingFunction(List<AlertSink> alertSinks,
                          AlertSeverity minSeverity,
                          boolean alertOnSuccess) {
        this.alertSinks = new ArrayList<>(alertSinks);
        this.minSeverity = minSeverity;
        this.alertOnSuccess = alertOnSuccess;
    }

    public AlertingFunction(AlertSink sink) {
        this(List.of(sink), AlertSeverity.WARNING, false);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        LOG.info("AlertingFunction opened with {} sinks", alertSinks.size());
    }

    @Override
    public void processElement(CheckResult result, Context ctx, Collector<Alert> out) {
        // Only alert on errors by default
        if (result.getStatus() == CheckStatus.SUCCESS && !alertOnSuccess) {
            return;
        }

        // Determine severity based on check result
        AlertSeverity severity = determineSeverity(result);

        // Only alert if severity meets threshold
        if (severity.ordinal() < minSeverity.ordinal()) {
            return;
        }

        // Create alert
        Alert alert = Alert.builder(UUID.randomUUID().toString())
            .severity(severity)
            .title("Data Quality Violation: " + result.getCheckName())
            .message(result.getDescription())
            .triggeringCheck(result)
            .timestamp(result.getTimestamp())
            .addContext("metric_value", result.getMetricValue())
            .addContext("metadata", result.getMetadata())
            .build();

        // Send to all configured sinks
        for (AlertSink sink : alertSinks) {
            try {
                sink.send(alert);
            } catch (Exception e) {
                LOG.error("Failed to send alert to sink: " + sink.getClass().getSimpleName(), e);
            }
        }

        // Emit alert to stream
        out.collect(alert);
    }

    private AlertSeverity determineSeverity(CheckResult result) {
        if (result.getStatus() == CheckStatus.SUCCESS) {
            return AlertSeverity.INFO;
        }

        // Use metric value to determine severity
        Double metricValue = result.getMetricValue();
        if (metricValue == null) {
            return AlertSeverity.WARNING;
        }

        if (metricValue < 0.5) {
            return AlertSeverity.CRITICAL;
        } else if (metricValue < 0.8) {
            return AlertSeverity.ERROR;
        } else {
            return AlertSeverity.WARNING;
        }
    }

    @Override
    public void close() throws Exception {
        for (AlertSink sink : alertSinks) {
            try {
                sink.close();
            } catch (Exception e) {
                LOG.error("Failed to close alert sink", e);
            }
        }
        super.close();
    }
}
