package com.streamdq.examples;

import com.streamdq.api.CheckResult;
import com.streamdq.window.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Comprehensive example demonstrating all fundamental windowed aggregated DQ checks.
 *
 * This showcases the core streaming DQ capabilities that distinguish StreamDQ
 * from batch frameworks:
 * - Windowed aggregation (not possible in per-record checks)
 * - Rate-based validation (% violations, not just pass/fail)
 * - Continuous monitoring over time
 * - Detection of gradual quality degradation
 *
 * Real-world scenario: E-commerce order stream validation
 */
public class ComprehensiveWindowedChecksExample {

    public static class Order {
        public String orderId;
        public String customerId;
        public String email;
        public String status;
        public Double amount;
        public String productSku;
        public Long timestamp;

        public Order() {}

        public Order(String orderId, String customerId, String email,
                    String status, Double amount, String productSku, Long timestamp) {
            this.orderId = orderId;
            this.customerId = customerId;
            this.email = email;
            this.status = status;
            this.amount = amount;
            this.productSku = productSku;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return String.format("Order{id='%s', customer='%s', email='%s', status='%s', amount=%.2f, sku='%s'}",
                orderId, customerId, email, status, amount, productSku);
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Simulate e-commerce order stream with various data quality issues
        DataStream<Order> orders = env.fromElements(
            // Good orders
            new Order("ORD001", "CUST001", "alice@example.com", "pending", 99.99, "SKU-12345", System.currentTimeMillis()),
            new Order("ORD002", "CUST002", "bob@example.com", "confirmed", 149.50, "SKU-67890", System.currentTimeMillis()),
            new Order("ORD003", "CUST003", "charlie@example.com", "pending", 75.25, "SKU-11111", System.currentTimeMillis()),

            // Issues to detect:
            new Order("ORD001", "CUST001", "alice@example.com", "pending", 99.99, "SKU-12345", System.currentTimeMillis()), // DUPLICATE order ID
            new Order("ORD004", "CUST004", "invalid-email", "confirmed", 200.00, "SKU-22222", System.currentTimeMillis()), // BAD EMAIL FORMAT
            new Order("ORD005", null, "dave@example.com", "pending", 50.00, "SKU-33333", System.currentTimeMillis()), // NULL customer ID
            new Order("ORD006", "CUST005", "eve@example.com", "INVALID_STATUS", 125.00, "SKU-44444", System.currentTimeMillis()), // INVALID STATUS
            new Order("ORD007", "CUST006", null, "confirmed", 88.88, "SKU-55555", System.currentTimeMillis()), // NULL EMAIL
            new Order("ORD008", "CUST007", "frank@example.com", "pending", -10.00, "SKU-66666", System.currentTimeMillis()), // NEGATIVE AMOUNT
            new Order("ORD009", "CUST008", "grace@", "confirmed", 175.00, "TOOLONG", System.currentTimeMillis()), // BAD EMAIL + SHORT SKU

            // More good orders
            new Order("ORD010", "CUST009", "henry@example.com", "shipped", 99.99, "SKU-77777", System.currentTimeMillis()),
            new Order("ORD011", "CUST010", "iris@example.com", "confirmed", 225.50, "SKU-88888", System.currentTimeMillis())
        );

        // Define 1-minute tumbling windows for aggregated DQ checks
        Time windowSize = Time.seconds(10); // Use 10 seconds for demo (would be minutes/hours in production)

        System.out.println("\n" + "=".repeat(80));
        System.out.println("COMPREHENSIVE WINDOWED DATA QUALITY CHECKS");
        System.out.println("=".repeat(80) + "\n");

        // 1. UNIQUENESS CHECK - Detect duplicate order IDs within window
        System.out.println("1. Uniqueness Check (Order ID)");
        System.out.println("-".repeat(40));
        DataStream<CheckResult> uniquenessResults = WindowedUniquenessCheck
            .<Order, ?>expectUnique("orderId", o -> o.orderId)
            .apply(orders
                .keyBy(o -> "all") // Single key to check globally
                .window(TumblingProcessingTimeWindows.of(windowSize)));

        uniquenessResults.print("Uniqueness");

        // 2. CARDINALITY CHECK - Track distinct customer count
        System.out.println("\n2. Cardinality Check (Customer Count)");
        System.out.println("-".repeat(40));
        DataStream<CheckResult> cardinalityResults = WindowedCardinalityCheck
            .<Order, ?>between("customerId", o -> o.customerId, 5L, 50L)
            .apply(orders
                .keyBy(o -> "all")
                .window(TumblingProcessingTimeWindows.of(windowSize)));

        cardinalityResults.print("Cardinality");

        // 3. PATTERN CONFORMANCE CHECK - Email format validation rate
        System.out.println("\n3. Pattern Conformance Check (Email Format)");
        System.out.println("-".repeat(40));
        DataStream<CheckResult> emailConformanceResults = WindowedPatternConformanceCheck
            .<Order, ?>email("email", o -> o.email, 0.90) // Allow 10% bad emails
            .apply(orders
                .keyBy(o -> "all")
                .window(TumblingProcessingTimeWindows.of(windowSize)));

        emailConformanceResults.print("Email Conformance");

        // 4. NULL RATE CHECK - Monitor null rate for critical fields
        System.out.println("\n4. Null Rate Check (Customer ID)");
        System.out.println("-".repeat(40));
        DataStream<CheckResult> nullRateResults = WindowedNullRateCheck
            .<Order, ?>on("customerId", o -> o.customerId, 0.05) // Max 5% nulls
            .apply(orders
                .keyBy(o -> "all")
                .window(TumblingProcessingTimeWindows.of(windowSize)));

        nullRateResults.print("Null Rate");

        // 5. DISTRIBUTION CHECK - Monitor status distribution
        System.out.println("\n5. Distribution Check (Order Status)");
        System.out.println("-".repeat(40));
        DataStream<CheckResult> distributionResults = WindowedDistributionCheck
            .<Order, ?>maxFrequency("status", o -> o.status, 0.70) // No single status > 70%
            .apply(orders
                .keyBy(o -> "all")
                .window(TumblingProcessingTimeWindows.of(windowSize)));

        distributionResults.print("Distribution");

        // 6. STRING LENGTH CHECK - Validate SKU format
        System.out.println("\n6. String Length Check (Product SKU)");
        System.out.println("-".repeat(40));
        DataStream<CheckResult> lengthResults = WindowedStringLengthCheck
            .<Order, ?>between("productSku", o -> o.productSku, 8, 10, 0.10) // 8-10 chars, max 10% violations
            .apply(orders
                .keyBy(o -> "all")
                .window(TumblingProcessingTimeWindows.of(windowSize)));

        lengthResults.print("String Length");

        // 7. RANGE CHECK - Amount validation (using existing WindowedRangeCheck)
        System.out.println("\n7. Range Check (Order Amount)");
        System.out.println("-".repeat(40));
        DataStream<CheckResult> rangeResults = WindowedRangeCheck
            .<Order, ?>on("amount", o -> o.amount, 0.0, 10000.0, 0.05) // Max 5% violations
            .apply(orders
                .keyBy(o -> "all")
                .window(TumblingProcessingTimeWindows.of(windowSize)));

        rangeResults.print("Range");

        // 8. AGGREGATE ALL RESULTS - Unified quality monitoring
        System.out.println("\n8. Unified Quality Dashboard");
        System.out.println("-".repeat(40));

        DataStream<CheckResult> allResults = uniquenessResults
            .union(cardinalityResults)
            .union(emailConformanceResults)
            .union(nullRateResults)
            .union(distributionResults)
            .union(lengthResults)
            .union(rangeResults);

        // Count failures by check type
        allResults
            .filter(r -> !r.isSuccess())
            .map(r -> String.format("❌ FAILURE: %s - %s", r.getCheckName(), r.getDescription()))
            .print("Quality Violations");

        // Calculate overall quality score
        allResults
            .map(r -> {
                double score = r.getMetricValue() != null ? r.getMetricValue() : 0.0;
                return String.format("📊 Check: %s | Score: %.2f%% | Status: %s",
                    r.getCheckName(),
                    score * 100,
                    r.getStatus());
            })
            .print("Quality Scores");

        System.out.println("\n" + "=".repeat(80));
        System.out.println("Starting Flink job...");
        System.out.println("=".repeat(80) + "\n");

        env.execute("Comprehensive Windowed DQ Checks Example");
    }
}
