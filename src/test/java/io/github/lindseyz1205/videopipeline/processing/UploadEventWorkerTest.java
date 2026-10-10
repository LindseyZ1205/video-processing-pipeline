package io.github.lindseyz1205.videopipeline.processing;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * The worker's two backoffs: how long a message waits after a failed attempt, and how long polling pauses after SQS
 * errors.
 */
class UploadEventWorkerTest {

    private static final Duration VISIBILITY_TIMEOUT = Duration.ofSeconds(60);
    private static final Duration MAX_RETRY_DELAY = Duration.ofMinutes(15);

    @Test
    void aFailedAttemptWaitsOneVisibilityTimeoutAfterItsFirstDelivery() {
        assertThat(retryDelay(1)).isEqualTo(Duration.ofSeconds(60));
    }

    @Test
    void theWaitDoublesWithEveryDelivery() {
        assertThat(retryDelay(2)).isEqualTo(Duration.ofSeconds(120));
        assertThat(retryDelay(3)).isEqualTo(Duration.ofSeconds(240));
        assertThat(retryDelay(4)).isEqualTo(Duration.ofSeconds(480));
    }

    @Test
    void theWaitStopsGrowingAtTheMaxRetryDelay() {
        assertThat(retryDelay(5)).isEqualTo(MAX_RETRY_DELAY);
        assertThat(retryDelay(100)).isEqualTo(MAX_RETRY_DELAY);
    }

    @Test
    void jitterAddsUpToAFifth() {
        assertThat(UploadEventWorker.retryDelay(2, VISIBILITY_TIMEOUT, MAX_RETRY_DELAY, 0.5))
                .isEqualTo(Duration.ofSeconds(132));
        assertThat(UploadEventWorker.retryDelay(2, VISIBILITY_TIMEOUT, MAX_RETRY_DELAY, 0.9999))
                .isEqualTo(Duration.ofSeconds(144));
    }

    @Test
    void anUnknownReceiveCountCountsAsTheFirstDelivery() {
        assertThat(retryDelay(0)).isEqualTo(Duration.ofSeconds(60));
    }

    @Test
    void pollingPausesUpToThirtySecondsAfterSqsErrors() {
        assertThat(UploadEventWorker.backoff(1)).isEqualTo(Duration.ofSeconds(1));
        assertThat(UploadEventWorker.backoff(2)).isEqualTo(Duration.ofSeconds(2));
        assertThat(UploadEventWorker.backoff(5)).isEqualTo(Duration.ofSeconds(16));
        assertThat(UploadEventWorker.backoff(6)).isEqualTo(Duration.ofSeconds(30));
        assertThat(UploadEventWorker.backoff(50)).isEqualTo(Duration.ofSeconds(30));
    }

    private static Duration retryDelay(int receiveCount) {
        return UploadEventWorker.retryDelay(receiveCount, VISIBILITY_TIMEOUT, MAX_RETRY_DELAY, 0);
    }
}
