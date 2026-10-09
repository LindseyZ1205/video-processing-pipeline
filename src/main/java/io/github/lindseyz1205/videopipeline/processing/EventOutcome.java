package io.github.lindseyz1205.videopipeline.processing;

import java.util.Locale;

/**
 * What happened to one S3 event record, and whether that lets the worker delete the SQS message.
 */
enum EventOutcome {

    PROCESSED(true),
    DUPLICATE(true),
    IGNORED(true),
    /** Permanent failure. Retrying cannot help, so the message does not use up SQS retries. */
    REJECTED(true),
    /** Another worker owns the job right now. */
    BUSY(false),
    /** Transient failure. The message stays so SQS retries it. */
    FAILED(false),
    /**
     * Handling stopped on an exception before an outcome was decided: the job store couldn't be reached, or the message
     * isn't a readable S3 event. The message stays, so SQS retries it.
     */
    ERROR(false);

    private final boolean deletesMessage;

    EventOutcome(boolean deletesMessage) {
        this.deletesMessage = deletesMessage;
    }

    boolean deletesMessage() {
        return deletesMessage;
    }

    /** Value of the {@code outcome} metric tag. */
    String tagValue() {
        return name().toLowerCase(Locale.ROOT);
    }
}
