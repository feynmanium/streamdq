package com.streamdq.anomaly;

import java.io.Serializable;

/**
 * Base interface for anomaly detection in streaming data.
 */
public interface AnomalyDetector extends Serializable {

    /**
     * Update the detector with a new value.
     */
    void update(double value);

    /**
     * Check if the given value is an anomaly.
     */
    boolean isAnomaly(double value);

    /**
     * Get the anomaly score (0.0 = normal, 1.0 = strong anomaly).
     */
    double getAnomalyScore(double value);

    /**
     * Reset the detector state.
     */
    void reset();
}
