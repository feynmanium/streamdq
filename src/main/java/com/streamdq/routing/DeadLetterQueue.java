package com.streamdq.routing;

import com.streamdq.api.CheckResult;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;

/**
 * Dead Letter Queue (DLQ) implementation for failed data quality checks.
 *
 * Critical for streaming DQ:
 * - Cannot stop stream processing for bad records
 * - Need to isolate bad data without blocking pipeline
 * - Enable offline investigation and replay
 * - Prevent downstream contamination
 *
 * @param <T> The type of data being routed
 */
public class DeadLetterQueue<T> extends ProcessFunction<DataWithCheckResult<T>, T> {
    private static final long serialVersionUID = 1L;

    private final OutputTag<DataWithCheckResult<T>> dlqTag;
    private final DLQStrategy strategy;

    public DeadLetterQueue(OutputTag<DataWithCheckResult<T>> dlqTag, DLQStrategy strategy) {
        this.dlqTag = dlqTag;
        this.strategy = strategy;
    }

    public DeadLetterQueue(OutputTag<DataWithCheckResult<T>> dlqTag) {
        this(dlqTag, DLQStrategy.ROUTE_ERRORS);
    }

    @Override
    public void processElement(DataWithCheckResult<T> value, Context ctx, Collector<T> out) {
        boolean shouldRoute = strategy.shouldRouteToD LQ(value.getCheckResult());

        if (shouldRoute) {
            // Send to DLQ
            ctx.output(dlqTag, value);
        } else {
            // Send to main output
            out.collect(value.getData());
        }
    }

    /**
     * Strategy for determining when to route to DLQ.
     */
    public enum DLQStrategy {
        /**
         * Route all records with ERROR status to DLQ.
         */
        ROUTE_ERRORS {
            @Override
            boolean shouldRouteToD LQ(CheckResult result) {
                return result.getStatus() == com.streamdq.api.CheckStatus.ERROR;
            }
        },

        /**
         * Route records with ERROR or WARNING status to DLQ.
         */
        ROUTE_ERRORS_AND_WARNINGS {
            @Override
            boolean shouldRouteToD LQ(CheckResult result) {
                return result.getStatus() != com.streamdq.api.CheckStatus.SUCCESS;
            }
        },

        /**
         * Route records with metric value below threshold to DLQ.
         */
        ROUTE_LOW_QUALITY {
            @Override
            boolean shouldRouteToD LQ(CheckResult result) {
                Double metricValue = result.getMetricValue();
                return metricValue != null && metricValue < 0.8;
            }
        };

        abstract boolean shouldRouteToD LQ(CheckResult result);
    }

    /**
     * Container for data along with its check result.
     */
    public static class DataWithCheckResult<T> implements Serializable {
        private static final long serialVersionUID = 1L;

        private final T data;
        private final CheckResult checkResult;
        private final long timestamp;

        public DataWithCheckResult(T data, CheckResult checkResult) {
            this.data = data;
            this.checkResult = checkResult;
            this.timestamp = System.currentTimeMillis();
        }

        public T getData() {
            return data;
        }

        public CheckResult getCheckResult() {
            return checkResult;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }

    public OutputTag<DataWithCheckResult<T>> getDlqTag() {
        return dlqTag;
    }
}
