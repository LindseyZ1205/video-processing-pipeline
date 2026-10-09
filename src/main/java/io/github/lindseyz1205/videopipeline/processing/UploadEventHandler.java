package io.github.lindseyz1205.videopipeline.processing;

import io.github.lindseyz1205.videopipeline.job.Job;
import io.github.lindseyz1205.videopipeline.job.JobStatus;
import io.github.lindseyz1205.videopipeline.job.JobStore;
import io.github.lindseyz1205.videopipeline.job.Lease;
import io.github.lindseyz1205.videopipeline.transcription.StoredObject;
import io.github.lindseyz1205.videopipeline.transcription.TranscriptionService;
import io.github.lindseyz1205.videopipeline.transcription.UnprocessableMediaException;
import io.github.lindseyz1205.videopipeline.upload.UploadKey;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.util.unit.DataSize;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

/**
 * Handles one SQS message carrying an S3 event, and decides whether the message can be deleted.
 *
 * <p>S3 notifications and SQS both deliver at least once, so the same upload can arrive several times, even on two
 * workers at once. The job record is the idempotency guard: only the worker that wins the conditional update in
 * {@link JobStore#tryAcquire} processes the file. Once the job is completed, later duplicates are deleted without
 * doing the work again. So are duplicates that arrive while it is still being processed, because the message that
 * claimed the job stays on the queue until the job is done.
 */
public class UploadEventHandler {

    private static final Logger log = LoggerFactory.getLogger(UploadEventHandler.class);

    private final JobStore jobs;
    private final S3Client s3;
    private final TranscriptionService transcription;
    private final PipelineMetrics metrics;
    private final long maxFileSizeBytes;

    public UploadEventHandler(JobStore jobs, S3Client s3, TranscriptionService transcription, PipelineMetrics metrics,
            DataSize maxFileSize) {
        this.jobs = jobs;
        this.s3 = s3;
        this.transcription = transcription;
        this.metrics = metrics;
        this.maxFileSizeBytes = maxFileSize.toBytes();
    }

    /** {@link #handle(String, String, LeaseKeeper)} without keeping leases alive: each lease keeps its first length. */
    public boolean handle(String messageId, String messageBody) {
        return handle(messageId, messageBody, LeaseKeeper.NONE);
    }

    /**
     * @param messageId the SQS message's ID, the same on every delivery of that message
     * @param leases    told when work on a job starts and ends, so the caller can keep the job's lease alive meanwhile
     * @return true when the message is fully handled and can be deleted; false to leave it on the queue so SQS
     *         delivers it again after the visibility timeout (and eventually moves it to the dead-letter queue)
     * @throws RuntimeException if the job store is unreachable or the message isn't a readable S3 event. The message
     *         stays on the queue in that case too, and the event counts as {@code error}, so the failure rate shows it.
     */
    public boolean handle(String messageId, String messageBody, LeaseKeeper leases) {
        try {
            return handle(messageId, S3EventNotification.parse(messageBody), leases);
        } catch (RuntimeException e) {
            metrics.recordOutcome(EventOutcome.ERROR);
            throw e;
        }
    }

    private boolean handle(String messageId, S3EventNotification notification, LeaseKeeper leases) {
        if (notification.isTestEvent()) {
            log.info("Ignoring the s3:TestEvent sent when the bucket notification was configured");
            return true;
        }
        boolean deleteMessage = true;
        for (S3EventNotification.EventRecord record : notification.records()) {
            EventOutcome outcome = process(record, messageId, leases);
            metrics.recordOutcome(outcome);
            deleteMessage &= outcome.deletesMessage();
        }
        return deleteMessage;
    }

    private EventOutcome process(S3EventNotification.EventRecord record, String messageId, LeaseKeeper leases) {
        if (!record.isObjectCreated()) {
            log.warn("Ignoring unexpected event {}", record.eventName());
            return EventOutcome.IGNORED;
        }
        String objectKey = record.objectKey();
        Optional<UploadKey> upload = UploadKey.parse(objectKey);
        if (upload.isEmpty()) {
            log.warn("Ignoring {}: not an upload created through the API", objectKey);
            return EventOutcome.IGNORED;
        }
        String jobId = upload.get().uploadId();
        // Every log line about this job carries its ID (a field of its own in structured logs).
        try (MDC.MDCCloseable ignored = MDC.putCloseable("jobId", jobId)) {
            return process(jobId, record, objectKey, messageId, leases);
        }
    }

    private EventOutcome process(String jobId, S3EventNotification.EventRecord record, String objectKey,
            String messageId, LeaseKeeper leases) {
        Optional<Lease> acquired = jobs.tryAcquire(jobId, record.bucketName(), objectKey, messageId);
        if (acquired.isEmpty()) {
            return lostClaim(jobId, messageId);
        }

        Lease lease = acquired.get();
        metrics.recordLag(record.eventTime());
        try {
            String transcript = transcribe(record.bucketName(), objectKey, lease, leases);
            if (!jobs.complete(lease, transcript)) {
                return EventOutcome.BUSY;
            }
            log.info("Job {} completed on attempt {}", jobId, lease.attempt());
            return EventOutcome.PROCESSED;
        } catch (UnprocessableMediaException e) {
            log.warn("Job {} failed permanently: {}", jobId, e.getMessage());
            jobs.fail(lease, e.getMessage());
            return EventOutcome.REJECTED;
        } catch (RuntimeException e) {
            log.warn("Job {} failed on attempt {}; SQS will redeliver the event", jobId, lease.attempt(), e);
            jobs.fail(lease, e.toString());
            return EventOutcome.FAILED;
        }
    }

    /**
     * The claim failed, so the job is completed or another delivery holds a live lease on it. Only a redelivery of the
     * message that holds the lease must stay on the queue. Any other message is a duplicate: the holding message stays
     * until the job is done, and comes back if its worker dies, so dropping the copy loses nothing. Kept, the copy
     * would come back busy every visibility timeout, and during a long transcription it would reach the dead-letter
     * queue.
     */
    private EventOutcome lostClaim(String jobId, String messageId) {
        Optional<Job> job = jobs.find(jobId);
        if (job.isPresent() && job.get().status() == JobStatus.COMPLETED) {
            log.info("Job {} is already completed; dropping the duplicate event", jobId);
            return EventOutcome.DUPLICATE;
        }
        String holder = job.map(Job::leaseMessageId).orElse(null);
        if (holder != null && !holder.equals(messageId)) {
            log.info("Job {} is being processed through message {}; dropping this duplicate", jobId, holder);
            return EventOutcome.DUPLICATE;
        }
        log.info("Job {} is held by another worker; checking again after the visibility timeout", jobId);
        return EventOutcome.BUSY;
    }

    /**
     * Keeps the lease alive only while the file is being transcribed. The lease is released before the result or the
     * failure is recorded, so a late heartbeat can't race with the write that ends it.
     */
    private String transcribe(String bucket, String objectKey, Lease lease, LeaseKeeper leases) {
        leases.keepAlive(lease);
        try {
            StoredObject object = describe(bucket, objectKey);
            if (object.sizeBytes() > maxFileSizeBytes) {
                throw new UnprocessableMediaException(
                        "File is " + object.sizeBytes() + " bytes, over the " + maxFileSizeBytes + " byte limit");
            }
            return metrics.timeTranscription(() -> transcription.transcribe(object));
        } finally {
            leases.release(lease);
        }
    }

    private StoredObject describe(String bucket, String key) {
        HeadObjectResponse head = s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build());
        return new StoredObject(bucket, key, Objects.requireNonNullElse(head.contentLength(), 0L), head.contentType());
    }
}
