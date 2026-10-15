package io.github.lindseyz1205.videopipeline.processing;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * The worker's two backoffs: how long a message waits after a failed attempt, and how long polling pauses after SQS
 * errors. The first depends on the queue's redrive policy, since the last delivery it allows doesn't wait at all.
 */
class UploadEventWorkerTest {

    private static final Duration VISIBILITY_TIMEOUT = Duration.ofSeconds(60);
    private static final Duration MAX_RETRY_DELAY = Duration.ofMinutes(15);
    /** A queue without a redrive policy, so no delivery is the last one. */
    private static final int NO_LIMIT = 0;

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
        assertThat(UploadEventWorker.retryDelay(2, NO_LIMIT, VISIBILITY_TIMEOUT, MAX_RETRY_DELAY, 0.5))
                .isEqualTo(Duration.ofSeconds(132));
        assertThat(UploadEventWorker.retryDelay(2, NO_LIMIT, VISIBILITY_TIMEOUT, MAX_RETRY_DELAY, 0.9999))
                .isEqualTo(Duration.ofSeconds(144));
    }

    @Test
    void theLastDeliveryGoesStraightToTheDeadLetterQueue() {
        assertThat(UploadEventWorker.retryDelay(2, 3, VISIBILITY_TIMEOUT, MAX_RETRY_DELAY, 0))
                .isEqualTo(Duration.ofSeconds(120));
        assertThat(UploadEventWorker.retryDelay(3, 3, VISIBILITY_TIMEOUT, MAX_RETRY_DELAY, 0)).isZero();
        assertThat(UploadEventWorker.retryDelay(4, 3, VISIBILITY_TIMEOUT, MAX_RETRY_DELAY, 0)).isZero();
    }

    @Test
    void theMaxReceiveCountComesFromTheRedrivePolicy() throws Exception {
        String deadLetterQueue = "arn:aws:sqs:us-east-1:000000000000:video-upload-events-dlq";
        assertThat(UploadEventWorker.maxReceiveCount(
                "{\"deadLetterTargetArn\":\"%s\",\"maxReceiveCount\":3}".formatted(deadLetterQueue))).isEqualTo(3);
        assertThat(UploadEventWorker.maxReceiveCount(
                "{\"deadLetterTargetArn\":\"%s\",\"maxReceiveCount\":\"5\"}".formatted(deadLetterQueue))).isEqualTo(5);
        assertThat(UploadEventWorker.maxReceiveCount("{}")).isZero();
        assertThat(UploadEventWorker.maxReceiveCount(null)).isZero();
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
        return UploadEventWorker.retryDelay(receiveCount, NO_LIMIT, VISIBILITY_TIMEOUT, MAX_RETRY_DELAY, 0);
    }
}
