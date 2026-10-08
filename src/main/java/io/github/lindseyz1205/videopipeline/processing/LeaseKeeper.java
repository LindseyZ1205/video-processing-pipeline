package io.github.lindseyz1205.videopipeline.processing;

import io.github.lindseyz1205.videopipeline.job.Lease;

/**
 * Told when work on a job starts and ends, so the caller can keep the job's lease alive in between. The worker passes
 * a {@link Heartbeat} for every message.
 */
public interface LeaseKeeper {

    /** Keeps nothing alive: the lease simply runs for its initial length. */
    LeaseKeeper NONE = new LeaseKeeper() {
        @Override
        public void keepAlive(Lease lease) {
        }

        @Override
        public void release(Lease lease) {
        }
    };

    /** The job has just been claimed and the work on it starts. */
    void keepAlive(Lease lease);

    /** The work on the job is over. Called before the result or the failure is recorded. */
    void release(Lease lease);
}
