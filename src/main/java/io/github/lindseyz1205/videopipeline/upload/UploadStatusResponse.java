package io.github.lindseyz1205.videopipeline.upload;

import io.github.lindseyz1205.videopipeline.job.Job;
import io.github.lindseyz1205.videopipeline.job.JobStatus;
import java.time.Instant;

public record UploadStatusResponse(
        String uploadId,
        JobStatus status,
        int attempts,
        String transcript,
        String error,
        Instant updatedAt) {

    static UploadStatusResponse from(Job job) {
        return new UploadStatusResponse(
                job.jobId(), job.status(), job.attempts(), job.transcript(), job.lastError(), job.updatedAt());
    }
}
