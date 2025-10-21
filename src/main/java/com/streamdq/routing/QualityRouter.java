package com.streamdq.routing;

import com.streamdq.api.CheckResult;
import com.streamdq.api.CheckStatus;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Routes data to different streams based on data quality check results.
 *
 * Enables:
 * - Valid data continues to main pipeline
 * - Failed data routed to DLQ
 * - Quarantined data for investigation
 * - Different processing paths for different quality levels
 *
 * @param <T> The type of data being routed
 */
public class QualityRouter<T> extends ProcessFunction<DeadLetterQueue.DataWithCheckResult<T>, T> {
    private static final long serialVersionUID = 1L;

    private final OutputTag<T> validTag;
    private final OutputTag<T> invalidTag;
    private final OutputTag<T> quarantineTag;

    public QualityRouter(OutputTag<T> validTag,
                        OutputTag<T> invalidTag,
                        OutputTag<T> quarantineTag) {
        this.validTag = validTag;
        this.invalidTag = invalidTag;
        this.quarantineTag = quarantineTag;
    }

    @Override
    public void processElement(DeadLetterQueue.DataWithCheckResult<T> value,
                              Context ctx,
                              Collector<T> out) {
        CheckResult result = value.getCheckResult();
        T data = value.getData();

        if (result.getStatus() == CheckStatus.SUCCESS) {
            // Valid data - continue in main stream
            ctx.output(validTag, data);
        } else if (result.getStatus() == CheckStatus.WARNING) {
            // Warning - may need investigation but not critical
            ctx.output(quarantineTag, data);
        } else {
            // Error - route to invalid stream
            ctx.output(invalidTag, data);
        }

        // Also emit to main stream for monitoring/metrics
        out.collect(data);
    }

    public OutputTag<T> getValidTag() {
        return validTag;
    }

    public OutputTag<T> getInvalidTag() {
        return invalidTag;
    }

    public OutputTag<T> getQuarantineTag() {
        return quarantineTag;
    }
}
