package com.streamdq.alerting;

/**
 * Severity levels for data quality alerts.
 */
public enum AlertSeverity {
    INFO,       // Informational, no action needed
    WARNING,    // Potential issue, should investigate
    ERROR,      // Definite issue, needs attention
    CRITICAL    // Severe issue, immediate action required
}
