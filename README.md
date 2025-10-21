# StreamDQ

**Data Quality Framework for Apache Flink - The AWS Deequ Equivalent for Streaming**

StreamDQ brings comprehensive data quality validation to Apache Flink streaming applications. Inspired by AWS Deequ (built for Spark), StreamDQ is designed from the ground up for streaming data with support for windowed aggregations, real-time anomaly detection, and production-grade metrics.

## Why StreamDQ?

AWS Deequ is the de-facto standard for data quality on Apache Spark, but there's been **no equivalent for Apache Flink** - until now. StreamDQ fills this gap with:

- **Streaming-First Design**: Built specifically for unbounded streams with window-based aggregations
- **Deequ-Compatible API**: Similar concepts (Checks, Constraints, Profiling) for easy migration
- **Real-Time Validation**: Check data quality as it flows, not in batch
- **Production Ready**: Metrics integration, state management, exactly-once processing
- **Extensible**: Easy to add custom checks and anomaly detectors

## Features

### Core Data Quality Checks

- **Completeness Checks**: Validate non-null values, similar to Deequ's completeness constraint
- **Pattern Checks**: Regex validation for emails, UUIDs, phone numbers, custom patterns
- **Range Checks**: Ensure numeric values fall within expected bounds
- **Custom Checks**: Define your own validation logic
- **Windowed Aggregations**: Check data quality over time windows, not just individual records

### Streaming-Specific Features

- **Window-Based Validation**: Apply checks over tumbling, sliding, or session windows
- **Stateful Checks**: Track uniqueness, anomalies across the entire stream
- **Side Outputs**: Separate success and error streams for different handling
- **Metrics Integration**: Export to Prometheus, Datadog, CloudWatch via Flink metrics

### Data Profiling

- **Statistical Profiling**: Compute min, max, mean, stddev over windows
- **Distribution Analysis**: Track data distributions in real-time
- **Completeness Tracking**: Monitor null rates and data completeness

### Anomaly Detection

- **Z-Score Detection**: Statistical anomaly detection using standard deviations
- **Moving Average Detection**: Detect sudden changes and spikes
- **Custom Detectors**: Implement your own anomaly detection algorithms

## Quick Start

### Add Dependency

```xml
<dependency>
    <groupId>com.streamdq</groupId>
    <artifactId>streamdq</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

### Basic Example

```java
import com.streamdq.api.*;
import com.streamdq.checks.*;
import org.apache.flink.streaming.api.datastream.DataStream;

// Define your data class
public class Transaction {
    public String id;
    public String customerId;
    public Double amount;
}

// Create data quality checks
DataQualityVerifier<Transaction> verifier = DataQualityVerifier.<Transaction>builder()
    .addCheck(CompletenessCheck.on("id", t -> t.id))
    .addCheck(CompletenessCheck.on("customerId", t -> t.customerId))
    .addCheck(RangeCheck.positive("amount", t -> t.amount))
    .addCheck(PatternCheck.on("customerId", t -> t.customerId, "CUST\\d+"))
    .build();

// Apply to your stream
DataStream<Transaction> transactions = ...;
DataStream<CheckResult> results = verifier.verify(transactions);

// Handle results
results.filter(r -> !r.isSuccess())
    .map(r -> "Data Quality Error: " + r.getDescription())
    .print();
```

### Windowed Checks

```java
import com.streamdq.window.*;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

// Check that 95% of records in each 1-minute window have non-null temperature
WindowedCompletenessCheck
    .<SensorReading, ?>on("temperature", s -> s.temperature, 0.95)
    .apply(sensorData
        .keyBy(s -> s.sensorId)
        .window(TumblingProcessingTimeWindows.of(Time.minutes(1))))
    .print();
```

### Data Profiling

```java
import com.streamdq.profiling.*;

// Profile numeric data over windows
NumericProfiler
    .<SensorReading, ?>on("temperature", s -> s.temperature)
    .profile(sensorData
        .keyBy(s -> s.sensorId)
        .window(TumblingProcessingTimeWindows.of(Time.minutes(5))))
    .map(profile -> {
        System.out.println("Min: " + profile.getMin());
        System.out.println("Max: " + profile.getMax());
        System.out.println("Mean: " + profile.getMean());
        System.out.println("StdDev: " + profile.getStdDev());
        System.out.println("Completeness: " + profile.getCompleteness());
        return profile;
    });
```

### Anomaly Detection

```java
import com.streamdq.anomaly.*;

// Detect anomalies using Z-score
metrics
    .keyBy(m -> m.metricName)
    .process(new AnomalyCheckFunction<>(
        "cpu_anomaly",
        m -> m.value,
        new ZScoreAnomalyDetector(3.0) // 3 standard deviations
    ))
    .filter(r -> !r.isSuccess())
    .print("Anomalies");
```

## Architecture

```
StreamDQ
├── api/                    # Core API (Check, CheckResult, DataQualityVerifier)
├── checks/                 # Built-in checks (Completeness, Pattern, Range, Custom)
├── window/                 # Windowed aggregation checks
├── profiling/              # Data profiling and statistics
├── metrics/                # Flink metrics integration
├── anomaly/                # Anomaly detection algorithms
└── utils/                  # Utility classes
```

## Comparison with AWS Deequ

| Feature | Deequ (Spark) | StreamDQ (Flink) |
|---------|--------------|------------------|
| Completeness Checks | ✅ | ✅ |
| Pattern Matching | ✅ | ✅ |
| Range Constraints | ✅ | ✅ |
| Custom Validations | ✅ | ✅ |
| Data Profiling | ✅ | ✅ |
| Anomaly Detection | ✅ | ✅ |
| **Streaming Windows** | ❌ | ✅ |
| **Real-Time Validation** | ❌ | ✅ |
| **Stateful Checks** | ❌ | ✅ |
| **Event Time Processing** | ❌ | ✅ |
| Batch Processing | ✅ | ✅ (via DataSet API) |

## Use Cases

1. **Real-Time Data Pipelines**: Validate data quality as it flows through Kafka, Kinesis, Pulsar
2. **IoT Data Validation**: Check sensor readings for completeness, range violations, anomalies
3. **Financial Transactions**: Ensure transaction data meets business rules in real-time
4. **Log Processing**: Validate log entries match expected patterns and schemas
5. **ML Feature Pipelines**: Ensure feature values are complete and within expected ranges

## Advanced Usage

### Side Outputs for Error Handling

```java
DataQualityVerifier.VerificationResult<Transaction> result =
    verifier.verifyWithSideOutputs(transactions);

// Route valid data to one sink
result.getSuccessResults()
    .map(r -> "Valid: " + r.getCheckName())
    .addSink(validSink);

// Route errors to another sink for investigation
result.getErrorResults()
    .map(r -> "Error: " + r.getDescription())
    .addSink(errorSink);
```

### Metrics Integration

```java
import com.streamdq.metrics.MetricsReporter;

// Automatically export metrics to Flink's metric system
results.process(new MetricsReporter());

// Configure Flink to export to Prometheus, Datadog, etc.
// See Flink metrics documentation
```

### Custom Checks

```java
import com.streamdq.checks.CustomCheck;

Check<Transaction> customCheck = CustomCheck.of(
    "business_rule",
    "Checks custom business logic",
    transaction -> {
        // Your custom validation logic
        return transaction.amount > 0 &&
               transaction.customerId != null &&
               transaction.customerId.startsWith("PREMIUM");
    },
    transaction -> "Business rule violated for: " + transaction.id
);
```

## Building from Source

```bash
git clone https://github.com/yourusername/streamdq.git
cd streamdq
mvn clean install
```

## Running Examples

```bash
cd examples
mvn exec:java -Dexec.mainClass="com.streamdq.examples.BasicUsageExample"
mvn exec:java -Dexec.mainClass="com.streamdq.examples.WindowedChecksExample"
mvn exec:java -Dexec.mainClass="com.streamdq.examples.AnomalyDetectionExample"
```

## Requirements

- Java 11+
- Apache Flink 1.18+
- Maven 3.6+

## Roadmap

- [ ] Table API integration for SQL-based checks
- [ ] Uniqueness checks with probabilistic data structures (HyperLogLog)
- [ ] Integration with Apache Iceberg for lakehouse validation
- [ ] Constraint suggestion engine (analyze data, suggest constraints)
- [ ] PyFlink bindings
- [ ] Web UI for monitoring data quality

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

Apache License 2.0

## Acknowledgments

Inspired by [AWS Deequ](https://github.com/awslabs/deequ) and built for the Apache Flink community.

## Related Projects

- [AWS Deequ](https://github.com/awslabs/deequ) - Data quality for Apache Spark
- [Great Expectations](https://greatexpectations.io/) - Python data validation
- [Apache Flink](https://flink.apache.org/) - Stream processing framework

---

**StreamDQ** - Bringing Deequ-like data quality to Apache Flink streaming 🚀