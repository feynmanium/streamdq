package com.streamdq.alerting;

import java.io.Serializable;

/**
 * Interface for sending alerts to external systems.
 *
 * Implementations might include:
 * - Slack notifications
 * - Email alerts
 * - PagerDuty incidents
 * - Webhook calls
 * - Kafka topic writes
 */
public interface AlertSink extends Serializable {

    /**
     * Send an alert to the configured destination.
     *
     * @param alert The alert to send
     * @throws Exception if sending fails
     */
    void send(Alert alert) throws Exception;

    /**
     * Close resources used by this sink.
     */
    default void close() throws Exception {
        // Default: no-op
    }
}
