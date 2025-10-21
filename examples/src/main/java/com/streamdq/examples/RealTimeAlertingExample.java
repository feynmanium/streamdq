package com.streamdq.examples;

import com.streamdq.alerting.*;
import com.streamdq.api.CheckResult;
import com.streamdq.api.DataQualityVerifier;
import com.streamdq.checks.CompletenessCheck;
import com.streamdq.checks.RangeCheck;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

/**
 * Example showing real-time alerting for data quality violations.
 *
 * Critical for streaming DQ:
 * - Immediate notification (not next-day batch reports)
 * - Integration with incident management
 * - Automated response to quality issues
 */
public class RealTimeAlertingExample {

    public static class PaymentTransaction {
        public String transactionId;
        public String accountId;
        public Double amount;
        public String currency;

        public PaymentTransaction() {}

        public PaymentTransaction(String transactionId, String accountId,
                                 Double amount, String currency) {
            this.transactionId = transactionId;
            this.accountId = accountId;
            this.amount = amount;
            this.currency = currency;
        }
    }

    /**
     * Custom alert sink that could integrate with Slack, PagerDuty, etc.
     */
    public static class CustomAlertSink implements AlertSink {
        private static final long serialVersionUID = 1L;

        @Override
        public void send(Alert alert) {
            // In production, this would:
            // - Send Slack notification
            // - Create PagerDuty incident
            // - Call webhook
            // - Write to Kafka topic
            // - Send email

            System.out.println("\n" + "=".repeat(80));
            System.out.println("🚨 REAL-TIME ALERT");
            System.out.println("=".repeat(80));
            System.out.println("Severity: " + alert.getSeverity());
            System.out.println("Title: " + alert.getTitle());
            System.out.println("Message: " + alert.getMessage());
            System.out.println("Time: " + alert.getTimestamp());
            if (alert.getTriggeringCheck() != null) {
                System.out.println("Check: " + alert.getTriggeringCheck().getCheckName());
                System.out.println("Metric: " + alert.getTriggeringCheck().getMetricValue());
            }
            System.out.println("Context: " + alert.getContext());
            System.out.println("=".repeat(80) + "\n");
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Simulate payment transaction stream
        DataStream<PaymentTransaction> transactions = env.fromElements(
            new PaymentTransaction("TX001", "ACC001", 100.50, "USD"),
            new PaymentTransaction("TX002", "ACC002", 250.00, "USD"),
            new PaymentTransaction("TX003", null, 50.00, "USD"),          // Missing account - ERROR
            new PaymentTransaction("TX004", "ACC003", -100.00, "USD"),    // Negative amount - ERROR
            new PaymentTransaction("TX005", "ACC004", 1000000.00, "USD"), // Suspicious amount - WARNING
            new PaymentTransaction("TX006", "ACC005", null, "USD"),       // Missing amount - ERROR
            new PaymentTransaction("TX007", "ACC006", 75.25, "USD")
        );

        // Define strict data quality checks for financial data
        DataQualityVerifier<PaymentTransaction> verifier = DataQualityVerifier.<PaymentTransaction>builder()
            .addCheck(CompletenessCheck.on("transactionId", t -> t.transactionId))
            .addCheck(CompletenessCheck.on("accountId", t -> t.accountId))
            .addCheck(CompletenessCheck.on("amount", t -> t.amount))
            .addCheck(RangeCheck.on("amount", t -> t.amount, 0.01, 100000.0))
            .build();

        // Apply checks
        DataStream<CheckResult> results = verifier.verify(transactions);

        // Configure multiple alert sinks
        List<AlertSink> alertSinks = List.of(
            new LoggingAlertSink(),
            new CustomAlertSink()
        );

        // Set up real-time alerting
        // Only alert on WARNING or higher severity
        AlertingFunction alerting = new AlertingFunction(
            alertSinks,
            AlertSeverity.WARNING,
            false  // Don't alert on success
        );

        DataStream<Alert> alerts = results
            .process(alerting)
            .name("Real-Time Alerting");

        // Monitor alert stream
        alerts
            .map(alert -> String.format("[%s] %s",
                alert.getSeverity(), alert.getTitle()))
            .print("Alerts");

        // Route critical alerts differently
        alerts
            .filter(alert -> alert.getSeverity() == AlertSeverity.CRITICAL)
            .map(alert -> "🔥 CRITICAL: " + alert.getMessage())
            .print("Critical Alerts");

        // Count alerts by severity
        alerts
            .map(alert -> alert.getSeverity().toString())
            .countWindowAll(10)
            .sum(0)
            .print("Alert Counts");

        env.execute("StreamDQ Real-Time Alerting Example");
    }
}
