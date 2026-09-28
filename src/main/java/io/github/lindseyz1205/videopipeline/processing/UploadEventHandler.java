package io.github.lindseyz1205.videopipeline.processing;

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
 * doing the work again.
 */
public class UploadEventHandler {

    private static final Logger log = LoggerFactory.getLogger(UploadEventHandler.class);

    private final JobStore jobs;
    private final S3Client s3;
    private final TranscriptionService transcription;
    private final long maxFileSizeBytes;

    public UploadEventHandler(JobStore jobs, S3Client s3, TranscriptionService transcription, DataSize maxFileSize) {
        this.jobs = jobs;
        this.s3 = s3;
        this.transcription = transcription;
        this.maxFileSizeBytes = maxFileSize.toBytes();
    }

    /**
     * @return true when the message is fully handled and can be deleted; false to leave it on the queue so SQS
     *         delivers it again after the visibility timeout (and eventually moves it to the dead-letter queue)
     * @throws RuntimeException if the job store or S3 is unreachable. The message stays on the queue in that case too.
     */
    public boolean handle(String messageBody) {
        S3EventNotification notification = S3EventNotification.parse(messageBody);
        if (notification.isTestEvent()) {
            log.info("Ignoring the s3:TestEvent sent when the bucket notification was configured");
            return true;
        }
        boolean deleteMessage = true;
        for (S3EventNotification.EventRecord record : notification.records()) {
            deleteMessage &= process(record).deletesMessage;
        }
        return deleteMessage;
    }

    private Outcome process(S3EventNotification.EventRecord record) {
        if (!record.isObjectCreated()) {
            log.warn("Ignoring unexpected event {}", record.eventName());
            return Outcome.IGNORED;
        }
        String objectKey = record.objectKey();
        Optional<UploadKey> upload = UploadKey.parse(objectKey);
        if (upload.isEmpty()) {
            log.warn("Ignoring {}: not an upload created through the API", objectKey);
            return Outcome.IGNORED;
        }
        String jobId = upload.get().uploadId();

        Optional<Lease> acquired = jobs.tryAcquire(jobId, record.bucketName(), objectKey);
        if (acquired.isEmpty()) {
            if (jobs.isCompleted(jobId)) {
                log.info("Job {} is already completed; dropping the duplicate event", jobId);
                return Outcome.DUPLICATE;
            }
            log.info("Job {} is held by another worker; checking again after the visibility timeout", jobId);
            return Outcome.BUSY;
        }

        Lease lease = acquired.get();
        try {
            StoredObject object = describe(record.bucketName(), objectKey);
            if (object.sizeBytes() > maxFileSizeBytes) {
                throw new UnprocessableMediaException(
                        "File is " + object.sizeBytes() + " bytes, over the " + maxFileSizeBytes + " byte limit");
            }
            String transcript = transcription.transcribe(object);
            if (!jobs.complete(lease, transcript)) {
                return Outcome.BUSY;
            }
            log.info("Job {} completed on attempt {}", jobId, lease.attempt());
            return Outcome.PROCESSED;
        } catch (UnprocessableMediaException e) {
            log.warn("Job {} failed permanently: {}", jobId, e.getMessage());
            jobs.fail(lease, e.getMessage());
            return Outcome.REJECTED;
        } catch (RuntimeException e) {
            log.warn("Job {} failed on attempt {}; SQS will redeliver the event", jobId, lease.attempt(), e);
            jobs.fail(lease, e.toString());
            return Outcome.FAILED;
        }
    }

    private StoredObject describe(String bucket, String key) {
        HeadObjectResponse head = s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build());
        return new StoredObject(bucket, key, Objects.requireNonNullElse(head.contentLength(), 0L), head.contentType());
    }

    private enum Outcome {
        PROCESSED(true),
        DUPLICATE(true),
        IGNORED(true),
        /** Permanent failure. Retrying cannot help, so the message does not use up SQS retries. */
        REJECTED(true),
        /** Another worker owns the job right now. */
        BUSY(false),
        /** Transient failure. The message stays so SQS retries it. */
        FAILED(false);

        private final boolean deletesMessage;

        Outcome(boolean deletesMessage) {
            this.deletesMessage = deletesMessage;
        }
    }
}
