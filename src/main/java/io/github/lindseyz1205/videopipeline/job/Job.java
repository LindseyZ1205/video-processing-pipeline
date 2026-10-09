package io.github.lindseyz1205.videopipeline.job;

import java.time.Instant;

/**
 * @param leaseMessageId while the job is being processed, the ID of the SQS message whose delivery holds the lease
 */
public record Job(
        String jobId,
        JobStatus status,
        int attempts,
        String objectKey,
        String transcript,
        String lastError,
        Instant updatedAt,
        String leaseMessageId) {
}
