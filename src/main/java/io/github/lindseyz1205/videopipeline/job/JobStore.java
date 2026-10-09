package io.github.lindseyz1205.videopipeline.job;

import java.util.Optional;

/**
 * Durable job state. It doubles as the idempotency record for upload events.
 */
public interface JobStore {

    /** Registers an upload whose file has not arrived yet. */
    void createPending(String jobId, String bucket, String objectKey);

    /**
     * Atomically claims the job for processing. This succeeds when the job is new, pending, failed, or processing
     * under an expired lease. It returns empty when the job is completed or another worker holds a live lease.
     *
     * @param messageId the SQS message being handled. The job keeps it while the lease is held, so a later delivery
     *                  can tell a duplicate from a redelivery of that same message.
     */
    Optional<Lease> tryAcquire(String jobId, String bucket, String objectKey, String messageId);

    /**
     * Pushes the lease's expiry out by another lease length, while the work on the job is still running. Returns false
     * if the lease was lost to another worker.
     */
    boolean extendLease(Lease lease);

    /** Stores the transcript and releases the lease. Returns false if the lease was lost to another worker. */
    boolean complete(Lease lease, String transcript);

    /** Records a failed attempt and releases the lease. Returns false if the lease was lost to another worker. */
    boolean fail(Lease lease, String error);

    /** A strongly consistent read, so a result that was just stored is always visible. */
    Optional<Job> find(String jobId);
}
