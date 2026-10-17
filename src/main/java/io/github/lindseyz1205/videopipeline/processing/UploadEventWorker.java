package io.github.lindseyz1205.videopipeline.processing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.lindseyz1205.videopipeline.config.PipelineProperties;
import io.github.lindseyz1205.videopipeline.job.JobStore;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
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
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

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
    /** SQS delays a message it is sent by at most 15 minutes. */
    private static final Duration MAX_SEND_DELAY = Duration.ofMinutes(15);
    /** How many times the event was sent again after the provider throttled it. */
    static final String THROTTLED_REQUEUES = "throttled-requeues";
    private static final ObjectMapper JSON = new ObjectMapper();

    private final SqsClient sqs;
    private final UploadEventHandler handler;
    private final JobStore jobs;
    private final PipelineMetrics metrics;
    private final Clock clock;
    private final String queueName;
    private final PipelineProperties.Worker settings;

    private volatile boolean running;
    private volatile String queueUrl;
    private volatile int maxReceiveCount;
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
        if (settings.maxThrottledRequeues() < 0) {
            throw new IllegalArgumentException("pipeline.worker.max-throttled-requeues ("
                    + settings.maxThrottledRequeues() + ") must not be negative");
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
        maxReceiveCount = readMaxReceiveCount();
        running = true;
        heartbeats = Executors.newScheduledThreadPool(settings.concurrency(), daemonThreads("upload-event-heartbeat-"));
        pollers = Executors.newFixedThreadPool(settings.concurrency(), daemonThreads("upload-event-poller-"));
        for (int i = 0; i < settings.concurrency(); i++) {
            pollers.execute(this::pollUntilStopped);
        }
        log.info("Consuming {} with {} poller thread(s), max receive count {}", queueUrl, settings.concurrency(),
                maxReceiveCount == 0 ? "none" : maxReceiveCount);
    }

    /**
     * How many deliveries the queue allows before SQS moves a message to the dead-letter queue. It comes from the
     * queue's own redrive policy rather than from configuration, so the two can't disagree; a changed policy is picked
     * up on the next start. 0 when the queue has no policy or it can't be read; then every failed attempt backs off.
     */
    private int readMaxReceiveCount() {
        GetQueueAttributesRequest request = GetQueueAttributesRequest.builder()
                .queueUrl(queueUrl)
                .attributeNames(QueueAttributeName.REDRIVE_POLICY)
                .build();
        try {
            String policy = sqs.getQueueAttributes(request).attributes().get(QueueAttributeName.REDRIVE_POLICY);
            return maxReceiveCount(policy);
        } catch (JsonProcessingException | RuntimeException e) {
            log.warn("Could not read the redrive policy of {}; every failed attempt backs off", queueUrl, e);
            return 0;
        }
    }

    /** The {@code maxReceiveCount} of a redrive policy, or 0 without one. */
    static int maxReceiveCount(String redrivePolicy) throws JsonProcessingException {
        // The count can be a JSON number or a string; asInt reads both.
        return redrivePolicy == null ? 0 : JSON.readTree(redrivePolicy).path("maxReceiveCount").asInt(0);
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
                                // How often the event was sent again after throttling, to stop at the limit.
                                .messageAttributeNames(THROTTLED_REQUEUES)
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
            Decision decision = handler.handle(message.messageId(), message.body(), heartbeat);
            heartbeat.close(); // the message's fate is decided; it must not be extended after this
            if (decision.outcome().deletesMessage()) {
                sqs.deleteMessage(DeleteMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .receiptHandle(message.receiptHandle())
                        .build());
            } else if (decision.outcome() == EventOutcome.FAILED) {
                retryLater(message, decision.retryAfter());
            }
        } catch (RuntimeException e) {
            // Not deleting is the safe default. SQS redelivers the message, and after maxReceiveCount deliveries it
            // moves to the dead-letter queue.
            log.warn("Message {} was not processed and will be redelivered", message.messageId(), e);
            heartbeat.close();
            retryLater(message, null);
        } finally {
            heartbeat.close();
        }
    }

    /**
     * Hides a message whose attempt failed for longer after each delivery, so a struggling provider or job store gets
     * room to recover instead of three quick retries.
     *
     * <p>When the provider throttled the attempt and said how long to wait, the event is sent again with that delay
     * instead, up to {@code max-throttled-requeues} times. Waiting out a rate limit then uses up none of its
     * deliveries, which are meant for attempts that failed.
     *
     * @param retryAfter how long the provider asked to wait, or {@code null}
     */
    private void retryLater(Message message, Duration retryAfter) {
        int receiveCount = receiveCount(message);
        double random = ThreadLocalRandom.current().nextDouble();
        if (retryAfter != null && throttledRequeues(message) < settings.maxThrottledRequeues()) {
            Duration copyDelay = requeueDelay(receiveCount, retryAfter, settings.visibilityTimeout(),
                    settings.maxRetryDelay(), random);
            if (requeue(message, copyDelay)) {
                return;
            }
        }
        Duration delay = retryDelay(receiveCount, maxReceiveCount, retryAfter, settings.visibilityTimeout(),
                settings.maxRetryDelay(), random);
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

    /**
     * Sends the event to the queue again, delayed, and deletes this delivery, so the copy starts over with a fresh
     * receive count. If the delete fails, both come back, and the job record keeps the work from being done twice.
     *
     * @return false if the copy couldn't be sent; the message then retries as a failed attempt
     */
    private boolean requeue(Message message, Duration delay) {
        int requeues = throttledRequeues(message) + 1;
        try {
            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(message.body())
                    .delaySeconds((int) delay.toSeconds())
                    .messageAttributes(Map.of(THROTTLED_REQUEUES, MessageAttributeValue.builder()
                            .dataType("Number")
                            .stringValue(Integer.toString(requeues))
                            .build()))
                    .build());
        } catch (RuntimeException e) {
            log.warn("Could not send throttled message {} again; it retries as a failed attempt",
                    message.messageId(), e);
            return false;
        }
        metrics.recordRequeued();
        log.info("The provider throttled message {}; sent it again with a {}s delay ({} of {})", message.messageId(),
                delay.toSeconds(), requeues, settings.maxThrottledRequeues());
        try {
            sqs.deleteMessage(DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build());
        } catch (RuntimeException e) {
            log.warn("Could not delete message {} after sending it again", message.messageId(), e);
        }
        return true;
    }

    /** How many times the event was sent again after throttling: 0 for the one S3 sent. */
    static int throttledRequeues(Message message) {
        MessageAttributeValue value = message.messageAttributes().get(THROTTLED_REQUEUES);
        if (value == null) {
            return 0;
        }
        try {
            return Integer.parseInt(value.stringValue());
        } catch (NumberFormatException e) {
            return Integer.MAX_VALUE; // counts as used up, so a bad value can't send an event around forever
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
     * second, four after the third, up to {@code maxDelay}. If the provider asked for a longer wait, the message
     * waits that long instead. A shorter one doesn't shorten the backoff, so a provider that keeps asking for one more
     * second can't use up every attempt within seconds. Up to 20% is added at random, so messages that failed
     * together, for example while the provider was rate limiting, don't all come back at the same moment.
     *
     * <p>After the last delivery the queue allows, there is no wait at all. The next receive moves the message to the
     * dead-letter queue, and waiting first would only delay that, and the alert with it.
     *
     * @param maxReceiveCount deliveries before SQS dead-letters the message; 0 if it never does
     * @param retryAfter      how long the provider asked to wait, or {@code null}
     * @param random          a number in {@code [0, 1)} that picks the jitter
     */
    static Duration retryDelay(int receiveCount, int maxReceiveCount, Duration retryAfter, Duration visibilityTimeout,
            Duration maxDelay, double random) {
        if (maxReceiveCount > 0 && receiveCount >= maxReceiveCount) {
            return Duration.ZERO;
        }
        int doublings = Math.min(Math.max(receiveCount, 1) - 1, 20);
        double millis = visibilityTimeout.toMillis() * Math.pow(2, doublings);
        if (retryAfter != null) {
            // At most maxDelay, rounded up to whole seconds, so below the cap it never comes before the provider asked.
            Duration asked = retryAfter.compareTo(maxDelay) > 0 ? maxDelay : retryAfter;
            millis = Math.max(millis, Math.ceil(asked.toMillis() / 1000.0) * 1000);
        }
        millis *= 1 + RETRY_JITTER * random;
        // SQS takes whole seconds. The base is whole seconds and jitter only adds, so rounding never goes below it.
        long seconds = Math.round(millis / 1000);
        return Duration.ofSeconds(Math.min(seconds, maxDelay.toSeconds()));
    }

    /**
     * The delay for an event sent again after throttling: the same wait as {@link #retryDelay}, except that the last
     * delivery waits too, since the copy starts over, and at most the 15 minutes SQS allows.
     */
    static Duration requeueDelay(int receiveCount, Duration retryAfter, Duration visibilityTimeout, Duration maxDelay,
            double random) {
        Duration delay = retryDelay(receiveCount, 0, retryAfter, visibilityTimeout, maxDelay, random);
        return delay.compareTo(MAX_SEND_DELAY) > 0 ? MAX_SEND_DELAY : delay;
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
