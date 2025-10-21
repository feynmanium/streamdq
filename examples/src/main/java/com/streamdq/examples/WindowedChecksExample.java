package com.streamdq.examples;

import com.streamdq.api.CheckResult;
import com.streamdq.profiling.DataProfile;
import com.streamdq.profiling.NumericProfiler;
import com.streamdq.window.WindowedCompletenessCheck;
import com.streamdq.window.WindowedRangeCheck;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Example showing windowed data quality checks and profiling.
 */
public class WindowedChecksExample {

    public static class SensorReading {
        public String sensorId;
        public Double temperature;
        public Double humidity;
        public Long timestamp;

        public SensorReading() {}

        public SensorReading(String sensorId, Double temperature, Double humidity, Long timestamp) {
            this.sensorId = sensorId;
            this.temperature = temperature;
            this.humidity = humidity;
            this.timestamp = timestamp;
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create a sample sensor data stream
        DataStream<SensorReading> sensorData = env.fromElements(
            new SensorReading("SENSOR001", 22.5, 45.0, System.currentTimeMillis()),
            new SensorReading("SENSOR001", 23.0, 46.5, System.currentTimeMillis()),
            new SensorReading("SENSOR001", null, 47.0, System.currentTimeMillis()), // Missing temperature
            new SensorReading("SENSOR001", 150.0, 48.0, System.currentTimeMillis()), // Out of range
            new SensorReading("SENSOR001", 22.8, 45.5, System.currentTimeMillis())
        );

        // Apply windowed completeness check
        // Check that at least 80% of records have non-null temperature values
        DataStream<CheckResult> completenessResults = WindowedCompletenessCheck
            .<SensorReading, ?>on("temperature", s -> s.temperature, 0.8)
            .apply(sensorData
                .keyBy(s -> s.sensorId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10))));

        completenessResults.print("Completeness Check");

        // Apply windowed range check
        // Check that temperature is between -10 and 50 degrees
        // Allow up to 10% violations
        DataStream<CheckResult> rangeResults = WindowedRangeCheck
            .<SensorReading, ?>on("temperature", s -> s.temperature, -10.0, 50.0, 0.1)
            .apply(sensorData
                .keyBy(s -> s.sensorId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10))));

        rangeResults.print("Range Check");

        // Profile numeric data
        DataStream<DataProfile> profiles = NumericProfiler
            .<SensorReading, ?>on("temperature", s -> s.temperature)
            .profile(sensorData
                .keyBy(s -> s.sensorId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10))));

        profiles.map(profile -> {
            System.out.println("=== Data Profile ===");
            System.out.println(profile);
            System.out.println("Completeness: " + (profile.getCompleteness() * 100) + "%");
            return profile;
        }).print("Profile");

        env.execute("StreamDQ Windowed Checks Example");
    }
}
