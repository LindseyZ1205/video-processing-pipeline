package io.github.lindseyz1205.videopipeline.upload;

/**
 * The idempotency key was already used for a request with a different file name, type or size. Returned as 422.
 */
public class IdempotencyKeyReusedException extends RuntimeException {

    public IdempotencyKeyReusedException() {
        super("This Idempotency-Key was already used for a different upload; use a new key for a new upload");
    }
}
