package com.streamdq.anomaly;

import com.streamdq.api.CheckResult;
import com.streamdq.api.CheckStatus;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.function.Function;

/**
 * Stateful anomaly detection for keyed streams.
 * Maintains separate anomaly detectors for each key.
 *
 * @param <K> Key type
 * @param <T> Input type
 */
public class AnomalyCheckFunction<K, T> extends KeyedProcessFunction<K, T, CheckResult> {
    private static final long serialVersionUID = 1L;

    private final String checkName;
    private final Function<T, Number> valueExtractor;
    private final AnomalyDetector detectorPrototype;

    private transient ValueState<AnomalyDetector> detectorState;

    public AnomalyCheckFunction(String checkName,
                                Function<T, Number> valueExtractor,
                                AnomalyDetector detectorPrototype) {
        this.checkName = checkName;
        this.valueExtractor = valueExtractor;
        this.detectorPrototype = detectorPrototype;
    }

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<AnomalyDetector> descriptor =
            new ValueStateDescriptor<>("anomaly-detector", AnomalyDetector.class);
        detectorState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(T value, Context ctx, Collector<CheckResult> out) throws Exception {
        // Get or create detector for this key
        AnomalyDetector detector = detectorState.value();
        if (detector == null) {
            // Clone the prototype (assuming it's a new instance)
            detector = createDetector();
            detectorState.update(detector);
        }

        Number numValue = valueExtractor.apply(value);
        if (numValue == null) {
            out.collect(CheckResult.builder("Anomaly_" + checkName)
                .status(CheckStatus.ERROR)
                .description("Null value encountered")
                .metricValue(0.0)
                .build());
            return;
        }

        double doubleValue = numValue.doubleValue();
        boolean isAnomaly = detector.isAnomaly(doubleValue);
        double anomalyScore = detector.getAnomalyScore(doubleValue);

        // Update detector with new value
        detector.update(doubleValue);
        detectorState.update(detector);

        // Emit check result
        CheckResult.Builder builder = CheckResult.builder("Anomaly_" + checkName)
            .metricValue(1.0 - anomalyScore)
            .addMetadata("value", doubleValue)
            .addMetadata("anomaly_score", anomalyScore);

        if (isAnomaly) {
            builder.status(CheckStatus.ERROR)
                .description(String.format("Anomaly detected: value=%.2f, score=%.2f",
                    doubleValue, anomalyScore));
        } else {
            builder.status(CheckStatus.SUCCESS)
                .description("No anomaly detected");
        }

        out.collect(builder.build());
    }

    private AnomalyDetector createDetector() {
        // Create a new instance of the same type
        if (detectorPrototype instanceof ZScoreAnomalyDetector) {
            return new ZScoreAnomalyDetector();
        } else if (detectorPrototype instanceof MovingAverageAnomalyDetector) {
            return new MovingAverageAnomalyDetector(100);
        }
        throw new IllegalStateException("Unknown detector type");
    }
}
