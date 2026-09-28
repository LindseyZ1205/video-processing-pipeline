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
     */
    Optional<Lease> tryAcquire(String jobId, String bucket, String objectKey);

    /** Stores the transcript and releases the lease. Returns false if the lease was lost to another worker. */
    boolean complete(Lease lease, String transcript);

    /** Records a failed attempt and releases the lease. Returns false if the lease was lost to another worker. */
    boolean fail(Lease lease, String error);

    boolean isCompleted(String jobId);

    Optional<Job> find(String jobId);
}
