package com.streamdq.api;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Splits CheckResults into success and error streams based on their status.
 */
public class CheckResultSplitter extends ProcessFunction<CheckResult, CheckResult> {
    private static final long serialVersionUID = 1L;

    private final OutputTag<CheckResult> errorTag;
    private final OutputTag<CheckResult> successTag;

    public CheckResultSplitter(OutputTag<CheckResult> errorTag, OutputTag<CheckResult> successTag) {
        this.errorTag = errorTag;
        this.successTag = successTag;
    }

    @Override
    public void processElement(CheckResult result,
                              Context ctx,
                              Collector<CheckResult> out) {
        // Output to main stream
        out.collect(result);

        // Output to side streams based on status
        if (result.getStatus() == CheckStatus.ERROR) {
            ctx.output(errorTag, result);
        } else {
            ctx.output(successTag, result);
        }
    }
}
