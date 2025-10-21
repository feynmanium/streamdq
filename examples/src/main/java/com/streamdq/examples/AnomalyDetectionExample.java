package com.streamdq.examples;

import com.streamdq.api.CheckResult;
import com.streamdq.anomaly.AnomalyCheckFunction;
import com.streamdq.anomaly.MovingAverageAnomalyDetector;
import com.streamdq.anomaly.ZScoreAnomalyDetector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Example showing anomaly detection in streaming data.
 */
public class AnomalyDetectionExample {

    public static class Metric {
        public String metricName;
        public Double value;
        public Long timestamp;

        public Metric() {}

        public Metric(String metricName, Double value, Long timestamp) {
            this.metricName = metricName;
            this.value = value;
            this.timestamp = timestamp;
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create a data stream with some anomalies
        DataStream<Metric> metrics = env.fromElements(
            new Metric("cpu_usage", 45.0, System.currentTimeMillis()),
            new Metric("cpu_usage", 47.0, System.currentTimeMillis()),
            new Metric("cpu_usage", 46.5, System.currentTimeMillis()),
            new Metric("cpu_usage", 48.0, System.currentTimeMillis()),
            new Metric("cpu_usage", 95.0, System.currentTimeMillis()), // ANOMALY!
            new Metric("cpu_usage", 46.0, System.currentTimeMillis()),
            new Metric("cpu_usage", 47.5, System.currentTimeMillis()),
            new Metric("cpu_usage", 15.0, System.currentTimeMillis()), // ANOMALY!
            new Metric("cpu_usage", 46.8, System.currentTimeMillis())
        );

        // Z-Score based anomaly detection
        DataStream<CheckResult> zScoreResults = metrics
            .keyBy(m -> m.metricName)
            .process(new AnomalyCheckFunction<>(
                "cpu_zscore",
                m -> m.value,
                new ZScoreAnomalyDetector(2.5) // 2.5 standard deviations
            ));

        zScoreResults
            .filter(r -> !r.isSuccess())
            .map(r -> "Z-Score Anomaly: " + r.getDescription())
            .print("Z-Score Detector");

        // Moving Average based anomaly detection
        DataStream<CheckResult> movingAvgResults = metrics
            .keyBy(m -> m.metricName)
            .process(new AnomalyCheckFunction<>(
                "cpu_moving_avg",
                m -> m.value,
                new MovingAverageAnomalyDetector(5, 0.3) // Window of 5, 30% deviation
            ));

        movingAvgResults
            .filter(r -> !r.isSuccess())
            .map(r -> "Moving Average Anomaly: " + r.getDescription())
            .print("Moving Average Detector");

        env.execute("StreamDQ Anomaly Detection Example");
    }
}
