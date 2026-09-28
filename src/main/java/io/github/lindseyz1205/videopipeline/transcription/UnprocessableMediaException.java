package io.github.lindseyz1205.videopipeline.transcription;

/**
 * The file itself is the problem (too large, unsupported format), so a retry would fail the same way. The worker
 * marks the job failed and drops the message instead of letting SQS redeliver it.
 */
public class UnprocessableMediaException extends RuntimeException {

    public UnprocessableMediaException(String message) {
        super(message);
    }
}
