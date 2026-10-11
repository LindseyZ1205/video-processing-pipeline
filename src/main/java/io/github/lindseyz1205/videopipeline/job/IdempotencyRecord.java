package io.github.lindseyz1205.videopipeline.job;

/**
 * What an idempotency key created: the upload's job, and a hash of the request that created it, so a later request
 * with the same key can be told apart from a retry.
 */
public record IdempotencyRecord(String jobId, String objectKey, String requestHash) {
}
