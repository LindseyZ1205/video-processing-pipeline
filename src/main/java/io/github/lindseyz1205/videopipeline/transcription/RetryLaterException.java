package io.github.lindseyz1205.videopipeline.transcription;

import java.time.Duration;

/**
 * The provider is rate limiting or briefly unavailable, and said how long to wait ({@code Retry-After}). The attempt
 * fails like any other transient failure, but the worker doesn't retry it before that wait is over.
 */
public class RetryLaterException extends RuntimeException {

    private final Duration retryAfter;

    public RetryLaterException(String message, Duration retryAfter, Throwable cause) {
        super(message, cause);
        this.retryAfter = retryAfter;
    }

    /** How long the provider asked to wait before the next request. */
    public Duration retryAfter() {
        return retryAfter;
    }
}
