package com.streamdq.alerting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple alert sink that logs alerts.
 * Useful for development and as a base for custom implementations.
 */
public class LoggingAlertSink implements AlertSink {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(LoggingAlertSink.class);

    @Override
    public void send(Alert alert) {
        switch (alert.getSeverity()) {
            case CRITICAL:
            case ERROR:
                LOG.error("DATA QUALITY ALERT: {}", alert);
                break;
            case WARNING:
                LOG.warn("DATA QUALITY ALERT: {}", alert);
                break;
            case INFO:
            default:
                LOG.info("DATA QUALITY ALERT: {}", alert);
                break;
        }
    }
}
