package io.github.lindseyz1205.videopipeline.processing;

import io.github.lindseyz1205.videopipeline.config.PipelineProperties;
import io.github.lindseyz1205.videopipeline.job.JobStore;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.context.SmartLifecycle;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

/**
 * Long-polls the upload event queue on a few threads and hands each message to {@link UploadEventHandler}.
 *
 * <p>Each thread finishes the messages it received before it polls again. A slow transcription therefore slows
 * consumption down instead of piling up in-flight work, and a {@link Heartbeat} keeps its message and job lease alive
 * while it runs, up to {@code max-processing-time}.
 */
public class UploadEventWorker implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(UploadEventWorker.class);
    private static final Duration MAX_BACKOFF = Duration.ofSeconds(30);
    /** SQS keeps a message hidden for at most 12 hours. */
    private static final Duration MAX_VISIBILITY = Duration.ofHours(12);
    /** Up to this share of a retry delay is added at random. */
    private static final double RETRY_JITTER = 0.2;

    private final SqsClient sqs;
    private final UploadEventHandler handler;
    private final JobStore jobs;
    private final PipelineMetrics metrics;
    private final Clock clock;
    private final String queueName;
    private final PipelineProperties.Worker settings;

    private volatile boolean running;
    private volatile String queueUrl;
    private ExecutorService pollers;
    private ScheduledExecutorService heartbeats;

    public UploadEventWorker(SqsClient sqs, UploadEventHandler handler, JobStore jobs, PipelineMetrics metrics,
            Clock clock, String queueName, PipelineProperties.Worker settings) {
        if (settings.heartbeatInterval().compareTo(settings.visibilityTimeout()) >= 0) {
            throw new IllegalArgumentException("pipeline.worker.heartbeat-interval (" + settings.heartbeatInterval()
                    + ") must be shorter than pipeline.worker.visibility-timeout (" + settings.visibilityTimeout() + ")");
        }
        if (settings.maxProcessingTime().compareTo(settings.visibilityTimeout()) <= 0) {
            throw new IllegalArgumentException("pipeline.worker.max-processing-time (" + settings.maxProcessingTime()
                    + ") must be longer than pipeline.worker.visibility-timeout (" + settings.visibilityTimeout() + ")");
        }
        if (settings.maxRetryDelay().compareTo(settings.visibilityTimeout()) < 0
                || settings.maxRetryDelay().compareTo(MAX_VISIBILITY) > 0) {
            throw new IllegalArgumentException("pipeline.worker.max-retry-delay (" + settings.maxRetryDelay()
                    + ") must be between pipeline.worker.visibility-timeout (" + settings.visibilityTimeout()
                    + ") and 12 hours");
        }
        this.sqs = sqs;
        this.handler = handler;
        this.jobs = jobs;
        this.metrics = metrics;
        this.clock = clock;
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
                                // How often the message has been delivered, to back off its retries.
                                .messageSystemAttributeNames(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT)
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
        Heartbeat heartbeat = new Heartbeat(sqs, jobs, metrics, heartbeats, clock, settings, queueUrl,
                message.receiptHandle());
        try (MDC.MDCCloseable ignored = MDC.putCloseable("messageId", message.messageId())) {
            EventOutcome outcome = handler.handle(message.messageId(), message.body(), heartbeat);
            heartbeat.close(); // the message's fate is decided; it must not be extended after this
            if (outcome.deletesMessage()) {
                sqs.deleteMessage(DeleteMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .receiptHandle(message.receiptHandle())
                        .build());
            } else if (outcome == EventOutcome.FAILED) {
                retryLater(message);
            }
        } catch (RuntimeException e) {
            // Not deleting is the safe default. SQS redelivers the message, and after maxReceiveCount deliveries it
            // moves to the dead-letter queue.
            log.warn("Message {} was not processed and will be redelivered", message.messageId(), e);
            heartbeat.close();
            retryLater(message);
        } finally {
            heartbeat.close();
        }
    }

    /**
     * Hides a message whose attempt failed for longer after each delivery, so a struggling provider or job store gets
     * room to recover instead of three quick retries.
     */
    private void retryLater(Message message) {
        Duration delay = retryDelay(receiveCount(message), settings.visibilityTimeout(), settings.maxRetryDelay(),
                ThreadLocalRandom.current().nextDouble());
        try {
            sqs.changeMessageVisibility(ChangeMessageVisibilityRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .visibilityTimeout((int) delay.toSeconds())
                    .build());
        } catch (RuntimeException e) {
            log.warn("Could not delay the retry of message {}; it comes back after the visibility timeout",
                    message.messageId(), e);
        }
    }

    private static int receiveCount(Message message) {
        String count = message.attributes().get(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT);
        try {
            return count == null ? 1 : Integer.parseInt(count);
        } catch (NumberFormatException e) {
            return 1;
        }
    }

    /**
     * How long a message waits after a failed attempt: one visibility timeout after its first delivery, two after the
     * second, four after the third, up to {@code maxDelay}. Up to 20% is added at random, so messages that failed
     * together, for example while the provider was rate limiting, don't all come back at the same moment.
     *
     * @param random a number in {@code [0, 1)} that picks the jitter
     */
    static Duration retryDelay(int receiveCount, Duration visibilityTimeout, Duration maxDelay, double random) {
        int doublings = Math.min(Math.max(receiveCount, 1) - 1, 20);
        double millis = visibilityTimeout.toMillis() * Math.pow(2, doublings) * (1 + RETRY_JITTER * random);
        // SQS takes whole seconds. Rounding never goes below the plain doubling, since jitter only adds.
        long seconds = Math.round(millis / 1000);
        return Duration.ofSeconds(Math.min(seconds, maxDelay.toSeconds()));
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
