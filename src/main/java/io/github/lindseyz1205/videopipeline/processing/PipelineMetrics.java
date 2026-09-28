package io.github.lindseyz1205.videopipeline.processing;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * The worker's metrics, exported at {@code /actuator/prometheus}:
 * <ul>
 *   <li>{@code pipeline.events} (counter, tag {@code outcome}): every S3 event record the worker handled</li>
 *   <li>{@code pipeline.transcription} (timer, tag {@code outcome}): time spent in the transcription provider</li>
 *   <li>{@code pipeline.events.lag} (timer): from the upload finishing to a worker starting on it, retries included</li>
 *   <li>{@code pipeline.sqs.receive.errors} (counter): failed polls of SQS</li>
 * </ul>
 * Queue depths are in {@link QueueDepthGauges}.
 */
public class PipelineMetrics {

    private final Clock clock;
    private final MeterRegistry registry;
    private final Map<EventOutcome, Counter> events = new EnumMap<>(EventOutcome.class);
    private final Timer transcriptionSucceeded;
    private final Timer transcriptionFailed;
    private final Timer lag;
    private final Counter receiveErrors;

    public PipelineMetrics(MeterRegistry registry, Clock clock) {
        this.registry = registry;
        this.clock = clock;
        // Registered up front, so a dashboard shows 0 instead of "no data" before the first occurrence.
        for (EventOutcome outcome : EventOutcome.values()) {
            events.put(outcome, Counter.builder("pipeline.events")
                    .description("S3 event records handled by the worker, by outcome")
                    .tag("outcome", outcome.tagValue())
                    .register(registry));
        }
        this.transcriptionSucceeded = transcriptionTimer("success");
        this.transcriptionFailed = transcriptionTimer("failure");
        this.lag = Timer.builder("pipeline.events.lag")
                .description("Time from the upload finishing to a worker starting on it, retries included")
                .register(registry);
        this.receiveErrors = Counter.builder("pipeline.sqs.receive.errors")
                .description("Failed SQS receive calls")
                .register(registry);
    }

    void recordOutcome(EventOutcome outcome) {
        events.get(outcome).increment();
    }

    /** @param eventTime when S3 created the event, i.e. when the upload finished; ignored if unknown */
    void recordLag(Instant eventTime) {
        if (eventTime == null) {
            return;
        }
        Duration sinceUpload = Duration.between(eventTime, clock.instant());
        if (!sinceUpload.isNegative()) {
            lag.record(sinceUpload);
        }
    }

    <T> T timeTranscription(Supplier<T> transcription) {
        Timer.Sample sample = Timer.start(registry);
        boolean succeeded = false;
        try {
            T result = transcription.get();
            succeeded = true;
            return result;
        } finally {
            sample.stop(succeeded ? transcriptionSucceeded : transcriptionFailed);
        }
    }

    void recordReceiveError() {
        receiveErrors.increment();
    }

    private Timer transcriptionTimer(String outcome) {
        return Timer.builder("pipeline.transcription")
                .description("Time spent in the transcription provider")
                .tag("outcome", outcome)
                .register(registry);
    }
}
