package io.github.lindseyz1205.videopipeline.job;

public enum JobStatus {

    /** The upload URL was handed out; the file has not arrived yet. */
    PENDING_UPLOAD,

    /** A worker holds the lease and is transcribing the file. */
    PROCESSING,

    COMPLETED,

    /**
     * The last attempt failed. After a transient failure SQS redelivers the event, until it moves to the dead-letter
     * queue; after a permanent one (e.g. file too large) the event is dropped.
     */
    FAILED
}
