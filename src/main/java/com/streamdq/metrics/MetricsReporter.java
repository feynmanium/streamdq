package com.streamdq.metrics;

import com.streamdq.api.CheckResult;
import com.streamdq.api.CheckStatus;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Reports data quality metrics to Flink's metrics system.
 * These can then be exported to Prometheus, Datadog, etc.
 */
public class MetricsReporter extends ProcessFunction<CheckResult, CheckResult> {
    private static final long serialVersionUID = 1L;

    private final ConcurrentHashMap<String, Counter> successCounters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> errorCounters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> lastMetricValues = new ConcurrentHashMap<>();

    private transient MetricGroup metricGroup;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        metricGroup = getRuntimeContext().getMetricGroup().addGroup("streamdq");
    }

    @Override
    public void processElement(CheckResult result, Context ctx, Collector<CheckResult> out) {
        String checkName = result.getCheckName();

        // Register metrics if not already registered
        Counter successCounter = successCounters.computeIfAbsent(
            checkName,
            name -> metricGroup.addGroup("checks", name).counter("success")
        );

        Counter errorCounter = errorCounters.computeIfAbsent(
            checkName,
            name -> metricGroup.addGroup("checks", name).counter("errors")
        );

        // Update counters
        if (result.getStatus() == CheckStatus.SUCCESS) {
            successCounter.inc();
        } else if (result.getStatus() == CheckStatus.ERROR) {
            errorCounter.inc();
        }

        // Update metric value gauge
        if (result.getMetricValue() != null) {
            AtomicLong metricValue = lastMetricValues.computeIfAbsent(
                checkName,
                name -> {
                    AtomicLong value = new AtomicLong();
                    metricGroup.addGroup("checks", name).gauge("metric_value",
                        (Gauge<Double>) () -> Double.longBitsToDouble(value.get()));
                    return value;
                }
            );
            metricValue.set(Double.doubleToLongBits(result.getMetricValue()));
        }

        // Pass through
        out.collect(result);
    }
}
