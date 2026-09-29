package io.github.lindseyz1205.videopipeline.processing;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

/**
 * {@code pipeline.queue.messages} gauges, tagged {@code queue} (events, dead-letter) and {@code state} (visible,
 * in-flight). A background refresh reads the counts from SQS, so a slow or failing SQS call never breaks a scrape. In
 * AWS the same numbers are SQS CloudWatch metrics, and that is where a dead-letter queue alarm would live.
 */
public class QueueDepthGauges implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(QueueDepthGauges.class);

    private final SqsClient sqs;
    private final Duration interval;
    private final Map<String, Depth> queues = new LinkedHashMap<>();
    private volatile ScheduledExecutorService refresher;

    public QueueDepthGauges(SqsClient sqs, MeterRegistry registry, String queueName, String deadLetterQueueName,
            Duration interval) {
        this.sqs = sqs;
        this.interval = interval;
        queues.put("events", new Depth(queueName));
        queues.put("dead-letter", new Depth(deadLetterQueueName));
        queues.forEach((queue, depth) -> {
            register(registry, queue, "visible", depth.visible);
            register(registry, queue, "in-flight", depth.inFlight);
        });
    }

    private static void register(MeterRegistry registry, String queue, String state, AtomicLong value) {
        Gauge.builder("pipeline.queue.messages", value, AtomicLong::get)
                .description("Approximate number of messages in the queue")
                .tags("queue", queue, "state", state)
                .register(registry);
    }

    /** Reads the current counts. A queue that can't be read keeps its last value. */
    void refresh() {
        for (Depth depth : queues.values()) {
            try {
                if (depth.url == null) {
                    depth.url = sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(depth.queueName).build()).queueUrl();
                }
                Map<QueueAttributeName, String> attributes = sqs.getQueueAttributes(GetQueueAttributesRequest.builder()
                                .queueUrl(depth.url)
                                .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES,
                                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE)
                                .build())
                        .attributes();
                depth.visible.set(count(attributes, QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES));
                depth.inFlight.set(count(attributes, QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE));
            } catch (RuntimeException e) {
                log.warn("Could not read the depth of queue {}: {}", depth.queueName, e.toString());
            }
        }
    }

    private static long count(Map<QueueAttributeName, String> attributes, QueueAttributeName name) {
        return Long.parseLong(attributes.getOrDefault(name, "0"));
    }

    @Override
    public void start() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "queue-depth-gauges");
            thread.setDaemon(true);
            return thread;
        });
        executor.scheduleWithFixedDelay(this::refresh, 0, interval.toMillis(), TimeUnit.MILLISECONDS);
        refresher = executor;
    }

    @Override
    public void stop() {
        ScheduledExecutorService executor = refresher;
        refresher = null;
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Override
    public boolean isRunning() {
        return refresher != null;
    }

    private static final class Depth {

        private final String queueName;
        private final AtomicLong visible = new AtomicLong();
        private final AtomicLong inFlight = new AtomicLong();
        private volatile String url;

        private Depth(String queueName) {
            this.queueName = queueName;
        }
    }
}
