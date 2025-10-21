# Streaming-Specific Data Quality Features

This document explains the streaming-specific features in StreamDQ that don't exist in batch data quality frameworks like Deequ.

## Why Streaming DQ is Different

Batch and streaming data quality have fundamentally different challenges:

| Aspect | Batch | Streaming |
|--------|-------|-----------|
| **Time** | Single dimension | Event time, processing time, ingestion time |
| **Completeness** | Dataset is complete | Never complete, need watermarks |
| **Order** | Can sort entire dataset | Data arrives out-of-order |
| **Lateness** | N/A | Data can arrive late |
| **State** | Ephemeral (per batch) | Long-lived, needs TTL |
| **Errors** | Fail and retry | Cannot stop stream, need DLQ |
| **Alerting** | Next-day reports | Real-time, millisecond latency |

## Streaming Features in StreamDQ

### 1. Event Time & Watermarks

**What**: Distinguish between when events occurred (event time) vs when they were processed (processing time).

**Why Critical**:
- Business logic based on when events happen, not when processed
- Enables correct results even with out-of-order data
- Allows replay with same results

**Example**:
```java
WatermarkStrategy<Event> watermarkStrategy = WatermarkStrategy
    .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
    .withTimestampAssigner((event, timestamp) -> event.eventTime);

DataStream<Event> eventsWithTimestamps = events
    .assignTimestampsAndWatermarks(watermarkStrategy);
```

**Use Cases**:
- IoT sensor data (event time = sensor reading time)
- Financial transactions (event time = transaction time)
- User activity logs (event time = action time)

---

### 2. Timeliness Checks

**What**: Validate that data is processed within acceptable time bounds.

**Why Critical**:
- Stale data = bad data in real-time systems
- Detect processing lag and upstream delays
- Ensure SLA compliance

**Example**:
```java
TimelinessCheck.maxLatency(
    "payment_timeliness",
    payment -> payment.getEventTime(),
    Duration.ofSeconds(30)  // Max 30 second delay
)
```

**Metrics Provided**:
- Event time
- Processing time
- Latency (difference)
- SLA violation flag

**Use Cases**:
- Payment processing (must be real-time)
- Fraud detection (delayed = useless)
- Real-time dashboards (freshness critical)

---

### 3. Allowed Lateness & Late Data Handling

**What**: Configure how long to wait for late-arriving data after watermark passes.

**Why Critical**:
- Real-world streams always have late data
- Network delays, system outages cause lateness
- Need to balance completeness vs latency

**Example**:
```java
OutputTag<Event> lateDataTag = new OutputTag<>("late-events") {};

windowedStream
    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
    .allowedLateness(Time.seconds(30))  // Wait 30s for late data
    .sideOutputLateData(lateDataTag)
    .aggregate(...);

// Process late data separately
stream.getSideOutput(lateDataTag)
    .addSink(lateDataSink);
```

**Configuration Options**:
- `Duration.ZERO` - Drop late data immediately (low latency)
- `Duration.ofMinutes(5)` - Wait 5 minutes (higher completeness)
- Send late data to side output or drop

**Use Cases**:
- Mobile apps (offline usage, sync later)
- Edge computing (intermittent connectivity)
- Multi-region aggregation (network delays)

---

### 4. State TTL (Time-To-Live)

**What**: Automatically clean up old state to prevent memory issues.

**Why Critical**:
- Unbounded streams = unbounded state
- Long-running jobs (days/weeks/months)
- Prevent OOM errors

**Example**:
```java
// Configure state to expire after 24 hours
StateTTLConfig ttlConfig = StateTTLConfig.Presets.oneDay();

// Use in stateful operations
ValueStateDescriptor<MyState> descriptor =
    new ValueStateDescriptor<>("my-state", MyState.class);
descriptor.enableTimeToLive(ttlConfig.getFlinkTtlConfig());
```

**Presets**:
- `oneHour()` - High-frequency metrics
- `oneDay()` - Daily aggregations
- `oneWeek()` - Weekly patterns
- `oneMonth()` - Monthly analytics

**Trade-offs**:
- Shorter TTL = Less memory, lose historical context
- Longer TTL = More memory, better analysis

**Use Cases**:
- User session tracking (expire after inactivity)
- Anomaly detection (keep recent history)
- Deduplication (finite time window)

---

### 5. Real-Time Alerting

**What**: Immediate notification when data quality violations occur.

**Why Critical**:
- Batch reports (next day) too late for streaming
- Need immediate response to issues
- Integration with incident management

**Example**:
```java
// Configure alert sinks
List<AlertSink> sinks = List.of(
    new SlackAlertSink(webhookUrl),
    new PagerDutyAlertSink(apiKey),
    new LoggingAlertSink()
);

// Set up real-time alerting
results.process(new AlertingFunction(
    sinks,
    AlertSeverity.WARNING,  // Minimum severity
    false  // Don't alert on success
));
```

**Alert Severities**:
- `INFO` - Informational, no action needed
- `WARNING` - Investigate when possible
- `ERROR` - Needs attention
- `CRITICAL` - Immediate action required

**Integration Points**:
- Slack/Teams notifications
- PagerDuty/OpsGenie incidents
- Email alerts
- Webhook calls
- Kafka topics

**Use Cases**:
- Payment processing failures
- Security violations
- SLA breaches
- System health issues

---

### 6. Dead Letter Queue (DLQ)

**What**: Route invalid data to separate stream for investigation without blocking pipeline.

**Why Critical**:
- Cannot stop stream processing for bad records
- Need to isolate without losing data
- Enable offline debugging
- Prevent downstream contamination

**Example**:
```java
OutputTag<DataWithCheckResult<Event>> dlqTag =
    new OutputTag<>("dlq") {};

SingleOutputStreamOperator<Event> validEvents = eventsWithChecks
    .process(new DeadLetterQueue<>(
        dlqTag,
        DLQStrategy.ROUTE_ERRORS
    ));

// Valid events continue to main pipeline
validEvents.addSink(productionSink);

// Invalid events go to DLQ for investigation
validEvents.getSideOutput(dlqTag)
    .addSink(dlqSink);
```

**Strategies**:
- `ROUTE_ERRORS` - Only errors to DLQ
- `ROUTE_ERRORS_AND_WARNINGS` - Errors + warnings
- `ROUTE_LOW_QUALITY` - Metric below threshold

**DLQ Destinations**:
- S3/HDFS for batch analysis
- Database for investigation
- Kafka topic for reprocessing
- Monitoring system

**Use Cases**:
- Malformed JSON/data
- Schema violations
- Business rule failures
- Data corruption

---

### 7. Multi-Way Quality Routing

**What**: Route data to different pipelines based on quality level.

**Why Critical**:
- Different quality = different processing
- Valid data to production
- Questionable data to quarantine
- Invalid data to DLQ

**Example**:
```java
OutputTag<Event> validTag = new OutputTag<>("valid") {};
OutputTag<Event> invalidTag = new OutputTag<>("invalid") {};
OutputTag<Event> quarantineTag = new OutputTag<>("quarantine") {};

SingleOutputStreamOperator<Event> routed = eventsWithChecks
    .process(new QualityRouter<>(
        validTag, invalidTag, quarantineTag
    ));

// Different sinks for different quality
routed.getSideOutput(validTag).addSink(productionSink);
routed.getSideOutput(invalidTag).addSink(dlqSink);
routed.getSideOutput(quarantineTag).addSink(reviewSink);
```

**Use Cases**:
- Multi-tier processing
- Confidence scoring
- Risk-based routing
- Compliance requirements

---

### 8. Event-Time Windowed Checks

**What**: Aggregate data quality metrics over event-time windows.

**Why Critical**:
- Processing time doesn't reflect reality
- Need correct results with out-of-order data
- Enable historical replay

**Example**:
```java
EventTimeWindowedCheck
    .<Event>on("field", e -> e.field, 0.95)  // 95% complete
    .applyWithLateDataHandling(
        events
            .keyBy(e -> e.key)
            .window(TumblingEventTimeWindows.of(Time.hours(1)))
            .allowedLateness(Time.minutes(5)),
        lateDataTag
    );
```

**Features**:
- Event-time aware aggregations
- Handles out-of-order data
- Configurable lateness
- Late data side outputs

**Use Cases**:
- Hourly/daily reports (exact time boundaries)
- Regulatory compliance (audit trail)
- Billing (accurate time attribution)

---

## Comparison: Streaming vs Batch DQ

### Completeness Check Example

**Batch (Deequ)**:
```scala
VerificationSuite()
  .onData(dataset)
  .addCheck(
    Check(CheckLevel.Error, "completeness")
      .isComplete("email")
  )
  .run()
```
- Scans entire dataset
- Returns single result
- Dataset is complete and immutable

**Streaming (StreamDQ)**:
```java
// Per-record completeness
CompletenessCheck.on("email", user -> user.email)

// Windowed completeness (more appropriate for streaming)
WindowedCompletenessCheck
    .on("email", user -> user.email, 0.95)
    .apply(users
        .keyBy(u -> u.region)
        .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
    )
```
- Continuous checking
- Windowed aggregations
- Results every window
- Handles infinite streams

### Key Differences

1. **Time Awareness**
   - Batch: No time dimension
   - Streaming: Event time, processing time, watermarks

2. **Completeness**
   - Batch: Exact (100% or not)
   - Streaming: Approximate (within window)

3. **Error Handling**
   - Batch: Fail batch, retry
   - Streaming: DLQ, continue processing

4. **Alerting**
   - Batch: Next-day reports
   - Streaming: Real-time, milliseconds

5. **State Management**
   - Batch: No persistent state
   - Streaming: State TTL, checkpointing

6. **Lateness**
   - Batch: N/A
   - Streaming: Watermarks, allowed lateness

## Best Practices

### 1. Choose Appropriate Window Sizes
- Too small: High overhead, incomplete picture
- Too large: Delayed feedback, high memory
- Rule of thumb: 1-5 minutes for operational metrics, 1 hour for analytics

### 2. Configure Watermarks Correctly
- `forBoundedOutOfOrderness()`: Know your max delay
- `forMonotonousTimestamps()`: Strictly ordered
- Add buffer: Max observed delay + safety margin

### 3. Set Reasonable Allowed Lateness
- Balance: Completeness vs result latency
- Monitor late data percentage
- Increase if dropping valid data
- Decrease if results too delayed

### 4. Use State TTL
- Always set TTL for unbounded keys (user IDs, etc.)
- Match TTL to business requirements
- Monitor state size metrics

### 5. Alert Smartly
- Use severity levels appropriately
- Avoid alert fatigue
- Aggregate similar alerts
- Include actionable context

### 6. DLQ Everything
- Never silently drop bad data
- Always route to DLQ
- Monitor DLQ volume
- Set up investigation pipeline

## Performance Considerations

### Memory Management
- State TTL prevents unbounded growth
- Windowing bounds memory usage
- DLQ prevents main pipeline slowdown

### Latency
- Watermarks add latency (buffering for out-of-order)
- Allowed lateness delays results
- Trade-off: Completeness vs speed

### Throughput
- Side outputs have minimal overhead
- Alerting is async
- Quality checks parallelize well

## Further Reading

- [Apache Flink Event Time](https://nightlies.apache.org/flink/flink-docs-master/docs/concepts/time/)
- [Watermark Strategies](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/event-time/generating_watermarks/)
- [State TTL](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/fault-tolerance/state/#state-time-to-live-ttl)
- [Side Outputs](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/side_output/)
