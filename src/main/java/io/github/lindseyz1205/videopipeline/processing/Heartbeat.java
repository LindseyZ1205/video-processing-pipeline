package io.github.lindseyz1205.videopipeline.processing;

import io.github.lindseyz1205.videopipeline.job.JobStore;
import io.github.lindseyz1205.videopipeline.job.Lease;
import java.time.Duration;
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
 */
class Heartbeat implements LeaseKeeper, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Heartbeat.class);

    private final SqsClient sqs;
    private final JobStore jobs;
    private final ScheduledExecutorService scheduler;
    private final String queueUrl;
    private final String receiptHandle;
    private final Duration visibilityTimeout;
    private final Duration interval;
    private final Set<Lease> leases = ConcurrentHashMap.newKeySet();

    private ScheduledFuture<?> beats;
    private volatile boolean stopped;

    Heartbeat(SqsClient sqs, JobStore jobs, ScheduledExecutorService scheduler, String queueUrl, String receiptHandle,
            Duration visibilityTimeout, Duration interval) {
        this.sqs = sqs;
        this.jobs = jobs;
        this.scheduler = scheduler;
        this.queueUrl = queueUrl;
        this.receiptHandle = receiptHandle;
        this.visibilityTimeout = visibilityTimeout;
        this.interval = interval;
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
                // A lease released meanwhile is finishing normally; only a lease still held can have been lost.
                if (!jobs.extendLease(lease) && leases.contains(lease)) {
                    log.warn("Lost the lease on job {} (attempt {}); another worker has taken it over, so the "
                            + "heartbeat stops", lease.jobId(), lease.attempt());
                    close();
                    return;
                }
            } catch (RuntimeException e) {
                log.warn("Could not extend the lease on job {}", lease.jobId(), e);
            }
        }
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
