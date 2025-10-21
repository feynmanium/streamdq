package com.streamdq.api;

/**
 * Status of a data quality check.
 */
public enum CheckStatus {
    SUCCESS,    // Check passed
    WARNING,    // Check passed but with warnings
    ERROR       // Check failed
}
