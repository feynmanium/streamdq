package com.streamdq.anomaly;

import java.util.LinkedList;
import java.util.Queue;

/**
 * Detects anomalies based on deviation from moving average.
 * Good for detecting sudden changes in streaming data.
 */
public class MovingAverageAnomalyDetector implements AnomalyDetector {
    private static final long serialVersionUID = 1L;

    private final int windowSize;
    private final double deviationThreshold; // Percentage deviation (e.g., 0.2 = 20%)
    private final Queue<Double> window;
    private double sum = 0.0;

    public MovingAverageAnomalyDetector(int windowSize, double deviationThreshold) {
        this.windowSize = windowSize;
        this.deviationThreshold = deviationThreshold;
        this.window = new LinkedList<>();
    }

    public MovingAverageAnomalyDetector(int windowSize) {
        this(windowSize, 0.3); // Default: 30% deviation
    }

    @Override
    public void update(double value) {
        window.offer(value);
        sum += value;

        if (window.size() > windowSize) {
            double removed = window.poll();
            sum -= removed;
        }
    }

    @Override
    public boolean isAnomaly(double value) {
        if (window.size() < windowSize / 2) {
            return false; // Not enough data
        }

        double movingAverage = sum / window.size();
        if (movingAverage == 0) {
            return value != 0;
        }

        double deviation = Math.abs(value - movingAverage) / movingAverage;
        return deviation > deviationThreshold;
    }

    @Override
    public double getAnomalyScore(double value) {
        if (window.size() < windowSize / 2) {
            return 0.0;
        }

        double movingAverage = sum / window.size();
        if (movingAverage == 0) {
            return value != 0 ? 1.0 : 0.0;
        }

        double deviation = Math.abs(value - movingAverage) / movingAverage;
        return Math.min(1.0, deviation / deviationThreshold);
    }

    @Override
    public void reset() {
        window.clear();
        sum = 0.0;
    }

    public double getMovingAverage() {
        return window.isEmpty() ? 0.0 : sum / window.size();
    }
}
