package com.streamdq.examples;

import com.streamdq.api.CheckResult;
import com.streamdq.api.DataQualityVerifier;
import com.streamdq.checks.CompletenessCheck;
import com.streamdq.checks.PatternCheck;
import com.streamdq.checks.RangeCheck;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Basic example showing how to use StreamDQ for data quality validation.
 */
public class BasicUsageExample {

    public static class Transaction {
        public String transactionId;
        public String customerId;
        public Double amount;
        public Long timestamp;

        public Transaction() {}

        public Transaction(String transactionId, String customerId, Double amount, Long timestamp) {
            this.transactionId = transactionId;
            this.customerId = customerId;
            this.amount = amount;
            this.timestamp = timestamp;
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create a sample data stream
        DataStream<Transaction> transactions = env.fromElements(
            new Transaction("TX001", "CUST001", 100.50, System.currentTimeMillis()),
            new Transaction("TX002", "CUST002", 250.00, System.currentTimeMillis()),
            new Transaction("TX003", null, 50.00, System.currentTimeMillis()), // Missing customer ID
            new Transaction("TX004", "CUST003", -10.00, System.currentTimeMillis()), // Invalid amount
            new Transaction(null, "CUST004", 300.00, System.currentTimeMillis()) // Missing transaction ID
        );

        // Define data quality checks
        DataQualityVerifier<Transaction> verifier = DataQualityVerifier.<Transaction>builder()
            // Check that transaction ID is not null
            .addCheck(CompletenessCheck.on("transactionId", t -> t.transactionId))

            // Check that customer ID is not null
            .addCheck(CompletenessCheck.on("customerId", t -> t.customerId))

            // Check that customer ID matches expected pattern (CUST followed by digits)
            .addCheck(PatternCheck.on("customerId", t -> t.customerId, "CUST\\d+"))

            // Check that amount is positive and less than 10,000
            .addCheck(RangeCheck.on("amount", t -> t.amount, 0.0, 10000.0))

            .build();

        // Apply checks and get results
        DataStream<CheckResult> results = verifier.verify(transactions);

        // Print all check results
        results.print();

        // Filter to only errors
        DataStream<CheckResult> errors = results.filter(CheckResult::isSuccess)
            .map(r -> {
                System.out.println("ERROR: " + r);
                return r;
            });

        env.execute("StreamDQ Basic Usage Example");
    }
}
