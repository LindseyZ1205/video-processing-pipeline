package io.github.lindseyz1205.videopipeline.processing;

import java.time.Duration;

/**
 * What {@link UploadEventHandler} decided about a message: the outcome that decides its fate and, when an attempt
 * failed, how long the transcription provider asked to wait before the next one.
 *
 * @param retryAfter the provider's {@code Retry-After}, or {@code null} if it gave none
 */
record Decision(EventOutcome outcome, Duration retryAfter) {

    static Decision of(EventOutcome outcome) {
        return new Decision(outcome, null);
    }
}
