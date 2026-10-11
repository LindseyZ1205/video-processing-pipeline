package io.github.lindseyz1205.videopipeline.job;

/**
 * Another request with the same idempotency key is being written at this very moment. Retrying shortly gets its result.
 */
public class IdempotencyKeyInUseException extends RuntimeException {

    public IdempotencyKeyInUseException() {
        super("A request with the same Idempotency-Key is still being processed; retry it");
    }
}
