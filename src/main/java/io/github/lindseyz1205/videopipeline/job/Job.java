package io.github.lindseyz1205.videopipeline.job;

import java.time.Instant;

public record Job(
        String jobId,
        JobStatus status,
        int attempts,
        String objectKey,
        String transcript,
        String lastError,
        Instant updatedAt) {
}
