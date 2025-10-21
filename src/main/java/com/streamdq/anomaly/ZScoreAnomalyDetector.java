package com.streamdq.anomaly;

/**
 * Detects anomalies using Z-score (standard deviations from mean).
 * Simple but effective for normally distributed data.
 */
public class ZScoreAnomalyDetector implements AnomalyDetector {
    private static final long serialVersionUID = 1L;

    private final double threshold; // Number of standard deviations
    private long count = 0;
    private double mean = 0.0;
    private double m2 = 0.0; // For Welford's algorithm

    public ZScoreAnomalyDetector(double threshold) {
        this.threshold = threshold;
    }

    public ZScoreAnomalyDetector() {
        this(3.0); // Default: 3 standard deviations
    }

    @Override
    public void update(double value) {
        count++;
        double delta = value - mean;
        mean += delta / count;
        double delta2 = value - mean;
        m2 += delta * delta2;
    }

    @Override
    public boolean isAnomaly(double value) {
        if (count < 2) {
            return false; // Not enough data
        }

        double stdDev = Math.sqrt(m2 / (count - 1));
        if (stdDev == 0) {
            return false; // No variance
        }

        double zScore = Math.abs(value - mean) / stdDev;
        return zScore > threshold;
    }

    @Override
    public double getAnomalyScore(double value) {
        if (count < 2) {
            return 0.0;
        }

        double stdDev = Math.sqrt(m2 / (count - 1));
        if (stdDev == 0) {
            return 0.0;
        }

        double zScore = Math.abs(value - mean) / stdDev;
        // Normalize to 0-1 range (capped at 1.0)
        return Math.min(1.0, zScore / (threshold * 2));
    }

    @Override
    public void reset() {
        count = 0;
        mean = 0.0;
        m2 = 0.0;
    }

    public double getMean() {
        return mean;
    }

    public double getStdDev() {
        return count > 1 ? Math.sqrt(m2 / (count - 1)) : 0.0;
    }
}
