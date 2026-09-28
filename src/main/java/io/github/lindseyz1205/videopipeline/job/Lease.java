package io.github.lindseyz1205.videopipeline.job;

/**
 * Proof that a worker currently owns a job. The token fences off stale workers: once a lease has expired and another
 * worker has taken the job over, the old worker can no longer write a result.
 */
public record Lease(String jobId, String token, int attempt) {
}
