package com.streamdq.examples;

import com.streamdq.api.CheckResult;
import com.streamdq.api.DataQualityVerifier;
import com.streamdq.checks.CompletenessCheck;
import com.streamdq.checks.PatternCheck;
import com.streamdq.routing.DeadLetterQueue;
import com.streamdq.routing.QualityRouter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

/**
 * Example showing Dead Letter Queue pattern for streaming DQ.
 *
 * Critical for streaming:
 * - Cannot stop stream for bad records
 * - Need to isolate invalid data
 * - Enable offline investigation
 * - Prevent downstream contamination
 */
public class DeadLetterQueueExample {

    public static class UserEvent {
        public String userId;
        public String email;
        public String action;

        public UserEvent() {}

        public UserEvent(String userId, String email, String action) {
            this.userId = userId;
            this.email = email;
            this.action = action;
        }

        @Override
        public String toString() {
            return String.format("UserEvent{userId='%s', email='%s', action='%s'}",
                userId, email, action);
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Simulate user event stream with various data quality issues
        DataStream<UserEvent> events = env.fromElements(
            new UserEvent("U001", "alice@example.com", "login"),
            new UserEvent("U002", "bob@example.com", "purchase"),
            new UserEvent("U003", "invalid-email", "login"),        // BAD EMAIL
            new UserEvent(null, "charlie@example.com", "logout"),   // MISSING USER ID
            new UserEvent("U004", null, "login"),                   // MISSING EMAIL
            new UserEvent("U005", "dave@", "purchase"),             // INVALID EMAIL
            new UserEvent("U006", "eve@example.com", "login")
        );

        // Define data quality checks
        DataQualityVerifier<UserEvent> verifier = DataQualityVerifier.<UserEvent>builder()
            .addCheck(CompletenessCheck.on("userId", e -> e.userId))
            .addCheck(CompletenessCheck.on("email", e -> e.email))
            .addCheck(PatternCheck.email("email", e -> e.email))
            .build();

        // Apply checks and attach results to data
        DataStream<DeadLetterQueue.DataWithCheckResult<UserEvent>> eventsWithChecks = events
            .map(new RichMapFunction<UserEvent, DeadLetterQueue.DataWithCheckResult<UserEvent>>() {
                @Override
                public DeadLetterQueue.DataWithCheckResult<UserEvent> map(UserEvent event) {
                    // Simulate running multiple checks
                    CheckResult result = verifier.verify(
                        env.fromElements(event)
                    ).executeAndCollect().next();

                    return new DeadLetterQueue.DataWithCheckResult<>(event, result);
                }
            });

        // Set up Dead Letter Queue
        OutputTag<DeadLetterQueue.DataWithCheckResult<UserEvent>> dlqTag =
            new OutputTag<DeadLetterQueue.DataWithCheckResult<UserEvent>>("dlq") {};

        SingleOutputStreamOperator<UserEvent> processedEvents = eventsWithChecks
            .process(new DeadLetterQueue<>(
                dlqTag,
                DeadLetterQueue.DLQStrategy.ROUTE_ERRORS
            ))
            .name("DLQ Router");

        // Main pipeline continues with valid data only
        processedEvents
            .map(event -> "✅ VALID: " + event)
            .print("Valid Events");

        // Dead letter queue receives invalid data
        DataStream<DeadLetterQueue.DataWithCheckResult<UserEvent>> dlq =
            processedEvents.getSideOutput(dlqTag);

        dlq.map(wrapper -> String.format("❌ DLQ: %s | Reason: %s",
                wrapper.getData(),
                wrapper.getCheckResult().getDescription()))
            .print("Dead Letter Queue");

        // Advanced: Multi-way routing
        OutputTag<UserEvent> validTag = new OutputTag<UserEvent>("valid") {};
        OutputTag<UserEvent> invalidTag = new OutputTag<UserEvent>("invalid") {};
        OutputTag<UserEvent> quarantineTag = new OutputTag<UserEvent>("quarantine") {};

        SingleOutputStreamOperator<UserEvent> routed = eventsWithChecks
            .process(new QualityRouter<>(validTag, invalidTag, quarantineTag))
            .name("Quality Router");

        // Different sinks for different quality levels
        routed.getSideOutput(validTag)
            .map(e -> "➡️  TO PRODUCTION: " + e)
            .print("Production Pipeline");

        routed.getSideOutput(invalidTag)
            .map(e -> "🗑️  TO DLQ: " + e)
            .print("Invalid Events");

        routed.getSideOutput(quarantineTag)
            .map(e -> "⚠️  TO QUARANTINE: " + e)
            .print("Quarantine (Needs Investigation)");

        env.execute("StreamDQ Dead Letter Queue Example");
    }
}
