package io.github.lindseyz1205.videopipeline.processing;

import io.github.lindseyz1205.videopipeline.config.PipelineProperties;
import io.github.lindseyz1205.videopipeline.job.JobStore;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.context.SmartLifecycle;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

/**
 * Long-polls the upload event queue on a few threads and hands each message to {@link UploadEventHandler}.
 *
 * <p>Each thread finishes the messages it received before it polls again. A slow transcription therefore slows
 * consumption down instead of piling up in-flight work, and a {@link Heartbeat} keeps its message and job lease alive
 * for as long as it runs.
 */
public class UploadEventWorker implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(UploadEventWorker.class);
    private static final Duration MAX_BACKOFF = Duration.ofSeconds(30);

    private final SqsClient sqs;
    private final UploadEventHandler handler;
    private final JobStore jobs;
    private final PipelineMetrics metrics;
    private final String queueName;
    private final PipelineProperties.Worker settings;

    private volatile boolean running;
    private volatile String queueUrl;
    private ExecutorService pollers;
    private ScheduledExecutorService heartbeats;

    public UploadEventWorker(SqsClient sqs, UploadEventHandler handler, JobStore jobs, PipelineMetrics metrics,
            String queueName, PipelineProperties.Worker settings) {
        if (settings.heartbeatInterval().compareTo(settings.visibilityTimeout()) >= 0) {
            throw new IllegalArgumentException("pipeline.worker.heartbeat-interval (" + settings.heartbeatInterval()
                    + ") must be shorter than pipeline.worker.visibility-timeout (" + settings.visibilityTimeout() + ")");
        }
        this.sqs = sqs;
        this.handler = handler;
        this.jobs = jobs;
        this.metrics = metrics;
        this.queueName = queueName;
        this.settings = settings;
    }

    @Override
    public void start() {
        queueUrl = sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build()).queueUrl();
        running = true;
        heartbeats = Executors.newScheduledThreadPool(settings.concurrency(), daemonThreads("upload-event-heartbeat-"));
        pollers = Executors.newFixedThreadPool(settings.concurrency(), daemonThreads("upload-event-poller-"));
        for (int i = 0; i < settings.concurrency(); i++) {
            pollers.execute(this::pollUntilStopped);
        }
        log.info("Consuming {} with {} poller thread(s)", queueUrl, settings.concurrency());
    }

    private void pollUntilStopped() {
        int consecutiveFailures = 0;
        while (running) {
            List<Message> messages;
            try {
                messages = sqs.receiveMessage(ReceiveMessageRequest.builder()
                                .queueUrl(queueUrl)
                                .maxNumberOfMessages(settings.maxMessages())
                                .waitTimeSeconds((int) settings.waitTime().toSeconds())
                                // Set per receive so the lease on the job always matches how long the message is hidden.
                                .visibilityTimeout((int) settings.visibilityTimeout().toSeconds())
                                .build())
                        .messages();
                consecutiveFailures = 0;
            } catch (RuntimeException e) {
                if (!running) {
                    break;
                }
                metrics.recordReceiveError();
                consecutiveFailures++;
                Duration backoff = backoff(consecutiveFailures);
                log.warn("Receiving from SQS failed {} time(s) in a row; retrying in {}s",
                        consecutiveFailures, backoff.toSeconds(), e);
                if (!sleep(backoff)) {
                    break;
                }
                continue;
            }
            messages.forEach(this::handle);
        }
    }

    private void handle(Message message) {
        Heartbeat heartbeat = new Heartbeat(sqs, jobs, heartbeats, queueUrl, message.receiptHandle(),
                settings.visibilityTimeout(), settings.heartbeatInterval());
        try (MDC.MDCCloseable ignored = MDC.putCloseable("messageId", message.messageId())) {
            boolean handled = handler.handle(message.body(), heartbeat);
            heartbeat.close(); // the message's fate is decided; it must not be extended after this
            if (handled) {
                sqs.deleteMessage(DeleteMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .receiptHandle(message.receiptHandle())
                        .build());
            }
        } catch (RuntimeException e) {
            // Not deleting is the safe default. SQS redelivers the message after the visibility timeout, and after
            // maxReceiveCount deliveries it moves to the dead-letter queue.
            log.warn("Message {} was not processed and will be redelivered", message.messageId(), e);
        } finally {
            heartbeat.close();
        }
    }

    static Duration backoff(int consecutiveFailures) {
        long seconds = 1L << Math.min(consecutiveFailures - 1, 5); // 1, 2, 4, ... 32
        return Duration.ofSeconds(Math.min(seconds, MAX_BACKOFF.toSeconds()));
    }

    private static boolean sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static ThreadFactory daemonThreads(String namePrefix) {
        AtomicInteger count = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, namePrefix + count.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }

    @Override
    public void stop() {
        running = false;
        if (pollers == null) {
            return;
        }
        pollers.shutdown();
        try {
            // Each poller exits after its current long poll and the messages it is handling.
            if (!pollers.awaitTermination(settings.waitTime().toSeconds() + 30, TimeUnit.SECONDS)) {
                pollers.shutdownNow();
            }
        } catch (InterruptedException e) {
            pollers.shutdownNow();
            Thread.currentThread().interrupt();
        } finally {
            heartbeats.shutdownNow();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public boolean isAutoStartup() {
        return settings.enabled();
    }
}
