package io.github.lindseyz1205.videopipeline.processing;

import io.github.lindseyz1205.videopipeline.config.PipelineProperties;
import io.github.lindseyz1205.videopipeline.job.JobStore;
import io.github.lindseyz1205.videopipeline.job.Lease;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;

/**
 * Keeps one SQS message, and the job leases taken while handling it, alive during long work.
 *
 * <p>Without it, a transcription that runs longer than the visibility timeout gets redelivered midway. Once its lease
 * runs out, another worker takes the job over and transcribes the file a second time. Every beat resets the message's
 * visibility timeout and extends each lease still in use. Beats start with the first lease and stop as soon as the
 * work ends, so a failed attempt still becomes visible again after the normal visibility timeout and SQS retries it
 * exactly as before.
 *
 * <p>Beats also stop once the message has been worked on for {@code max-processing-time}. A transcription that hangs
 * would otherwise keep its job forever. This way its lease runs out, and the next delivery takes the job over.
 */
class Heartbeat implements LeaseKeeper, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Heartbeat.class);

    private final SqsClient sqs;
    private final JobStore jobs;
    private final PipelineMetrics metrics;
    private final ScheduledExecutorService scheduler;
    private final Clock clock;
    private final String queueUrl;
    private final String receiptHandle;
    private final Duration visibilityTimeout;
    private final Duration interval;
    private final Duration maxProcessingTime;
    private final Instant deadline;
    private final Set<Lease> leases = ConcurrentHashMap.newKeySet();

    private ScheduledFuture<?> beats;
    private volatile boolean stopped;

    Heartbeat(SqsClient sqs, JobStore jobs, PipelineMetrics metrics, ScheduledExecutorService scheduler, Clock clock,
            PipelineProperties.Worker settings, String queueUrl, String receiptHandle) {
        this.sqs = sqs;
        this.jobs = jobs;
        this.metrics = metrics;
        this.scheduler = scheduler;
        this.clock = clock;
        this.queueUrl = queueUrl;
        this.receiptHandle = receiptHandle;
        this.visibilityTimeout = settings.visibilityTimeout();
        this.interval = settings.heartbeatInterval();
        this.maxProcessingTime = settings.maxProcessingTime();
        this.deadline = clock.instant().plus(maxProcessingTime);
    }

    @Override
    public synchronized void keepAlive(Lease lease) {
        if (stopped) {
            return;
        }
        leases.add(lease);
        if (beats == null) {
            long millis = interval.toMillis();
            beats = scheduler.scheduleAtFixedRate(this::beat, millis, millis, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void release(Lease lease) {
        leases.remove(lease);
    }

    /** One beat: extend the message, then every lease that is still in use. */
    void beat() {
        if (stopped || leases.isEmpty()) {
            return;
        }
        if (!clock.instant().isBefore(deadline)) {
            giveUp();
            return;
        }
        try {
            sqs.changeMessageVisibility(ChangeMessageVisibilityRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(receiptHandle)
                    .visibilityTimeout((int) visibilityTimeout.toSeconds())
                    .build());
        } catch (RuntimeException e) {
            log.warn("Could not extend the visibility of the message being processed", e);
        }
        for (Lease lease : leases) {
            try {
                if (jobs.extendLease(lease)) {
                    metrics.recordLeaseExtended();
                } else if (leases.contains(lease)) {
                    // A lease released meanwhile is finishing normally; only a lease still held can have been lost.
                    metrics.recordLeaseLost();
                    log.warn("Lost the lease on job {} (attempt {}); another worker has taken it over, so the "
                            + "heartbeat stops", lease.jobId(), lease.attempt());
                    close();
                    return;
                }
            } catch (RuntimeException e) {
                metrics.recordLeaseExtensionFailed();
                log.warn("Could not extend the lease on job {}", lease.jobId(), e);
            }
        }
    }

    /** The work has run too long. Stops extending it, so its leases run out and the next delivery takes over. */
    private void giveUp() {
        for (Lease lease : leases) {
            metrics.recordLeaseAbandoned();
            log.warn("Job {} (attempt {}) ran past pipeline.worker.max-processing-time ({}); no longer extending "
                    + "its lease, so the next delivery can take it over", lease.jobId(), lease.attempt(),
                    maxProcessingTime);
        }
        close();
    }

    /** Stops the beats. Safe to call more than once. */
    @Override
    public synchronized void close() {
        stopped = true;
        if (beats != null) {
            beats.cancel(false);
        }
    }
}
