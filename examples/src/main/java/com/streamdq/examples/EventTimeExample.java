package com.streamdq.examples;

import com.streamdq.api.CheckResult;
import com.streamdq.time.EventTimeConfig;
import com.streamdq.time.TimelinessCheck;
import com.streamdq.window.EventTimeWindowedCheck;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.time.Instant;

/**
 * Example showing event time, watermarks, and late data handling.
 *
 * This demonstrates critical streaming DQ features that don't exist in batch:
 * - Event time vs processing time
 * - Watermark generation
 * - Allowed lateness configuration
 * - Late data side outputs
 * - Timeliness checks
 */
public class EventTimeExample {

    public static class IoTEvent implements EventTimeWindowedCheck.EventTimeAware {
        public String sensorId;
        public Double temperature;
        public Long eventTime; // Unix timestamp in millis

        public IoTEvent() {}

        public IoTEvent(String sensorId, Double temperature, Long eventTime) {
            this.sensorId = sensorId;
            this.temperature = temperature;
            this.eventTime = eventTime;
        }

        @Override
        public long getEventTime() {
            return eventTime;
        }

        public Instant getEventTimeAsInstant() {
            return Instant.ofEpochMilli(eventTime);
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        long now = System.currentTimeMillis();

        // Create events with various event times (some out of order, some late)
        DataStream<IoTEvent> events = env.fromElements(
            new IoTEvent("sensor-1", 22.5, now - 10000),  // 10 seconds ago
            new IoTEvent("sensor-1", 23.0, now - 8000),   // 8 seconds ago
            new IoTEvent("sensor-1", null, now - 6000),   // 6 seconds ago, missing value
            new IoTEvent("sensor-1", 22.8, now - 15000),  // 15 seconds ago - OUT OF ORDER!
            new IoTEvent("sensor-1", 23.5, now - 2000),   // 2 seconds ago
            new IoTEvent("sensor-1", 22.9, now - 20000)   // 20 seconds ago - VERY LATE!
        );

        // Configure event time and watermarks
        WatermarkStrategy<IoTEvent> watermarkStrategy = WatermarkStrategy
            .<IoTEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withTimestampAssigner((SerializableTimestampAssigner<IoTEvent>)
                (event, timestamp) -> event.eventTime);

        // Assign timestamps and watermarks
        DataStream<IoTEvent> eventsWithTimestamps = events
            .assignTimestampsAndWatermarks(watermarkStrategy);

        // 1. Check timeliness - ensure events are processed within 30 seconds
        DataStream<CheckResult> timelinessResults = eventsWithTimestamps
            .map(TimelinessCheck.maxLatency(
                "iot_timeliness",
                IoTEvent::getEventTimeAsInstant,
                Duration.ofSeconds(30)
            ).toMapFunction())
            .name("Timeliness Check");

        timelinessResults
            .filter(r -> !r.isSuccess())
            .map(r -> "LATE EVENT: " + r.getDescription())
            .print("Timeliness Violations");

        // 2. Event time windowed completeness check with late data handling
        OutputTag<IoTEvent> lateDataTag = new OutputTag<IoTEvent>("late-iot-events") {};

        SingleOutputStreamOperator<CheckResult> windowedResults =
            (SingleOutputStreamOperator<CheckResult>) EventTimeWindowedCheck
                .<IoTEvent>on("temperature", e -> e.temperature, 0.9)
                .applyWithLateDataHandling(
                    eventsWithTimestamps
                        .keyBy(e -> e.sensorId)
                        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                        .allowedLateness(Time.seconds(5)),  // Allow 5 seconds of lateness
                    lateDataTag
                ).getResults();

        windowedResults.print("Window Completeness");

        // 3. Access late data from side output
        DataStream<IoTEvent> lateData = windowedResults.getSideOutput(lateDataTag);
        lateData
            .map(e -> String.format("LATE DATA: sensor=%s, temp=%.1f, eventTime=%s, lateness=%dms",
                e.sensorId, e.temperature,
                Instant.ofEpochMilli(e.eventTime),
                System.currentTimeMillis() - e.eventTime))
            .print("Late Events");

        // 4. Monitor watermark progress (for debugging)
        eventsWithTimestamps
            .map(e -> String.format("Event: time=%s, temp=%.1f",
                Instant.ofEpochMilli(e.eventTime), e.temperature))
            .print("Events");

        env.execute("StreamDQ Event Time Example");
    }
}
