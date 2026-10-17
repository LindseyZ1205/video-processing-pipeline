package io.github.lindseyz1205.videopipeline.processing;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

/**
 * The worker's two backoffs: how long a message waits after a failed attempt, and how long polling pauses after SQS
 * errors. The first depends on the queue's redrive policy, since the last delivery it allows doesn't wait at all.
 */
class UploadEventWorkerTest {

    private static final Duration VISIBILITY_TIMEOUT = Duration.ofSeconds(60);
    private static final Duration MAX_RETRY_DELAY = Duration.ofMinutes(15);
    /** A queue without a redrive policy, so no delivery is the last one. */
    private static final int NO_LIMIT = 0;
    /** The provider didn't say how long to wait. */
    private static final Duration NO_RETRY_AFTER = null;

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
        assertThat(retryDelay(2, NO_LIMIT, NO_RETRY_AFTER, 0.5)).isEqualTo(Duration.ofSeconds(132));
        assertThat(retryDelay(2, NO_LIMIT, NO_RETRY_AFTER, 0.9999)).isEqualTo(Duration.ofSeconds(144));
    }

    @Test
    void theLastDeliveryGoesStraightToTheDeadLetterQueue() {
        assertThat(retryDelay(2, 3, NO_RETRY_AFTER, 0)).isEqualTo(Duration.ofSeconds(120));
        assertThat(retryDelay(3, 3, NO_RETRY_AFTER, 0)).isZero();
        assertThat(retryDelay(4, 3, NO_RETRY_AFTER, 0)).isZero();
    }

    @Test
    void theProviderCanAskForALongerWait() {
        assertThat(retryDelay(1, 3, Duration.ofMinutes(5), 0)).isEqualTo(Duration.ofMinutes(5));
        // Rounded up, so the retry never comes before the provider asked; the jitter still adds up to a fifth.
        assertThat(retryDelay(1, 3, Duration.ofMillis(90_001), 0)).isEqualTo(Duration.ofSeconds(91));
        assertThat(retryDelay(1, 3, Duration.ofMinutes(5), 0.5)).isEqualTo(Duration.ofSeconds(330));
    }

    @Test
    void theProviderCannotShortenTheWaitOrLiftTheCap() {
        assertThat(retryDelay(2, 3, Duration.ofSeconds(1), 0)).isEqualTo(Duration.ofSeconds(120));
        assertThat(retryDelay(1, 3, Duration.ofDays(1), 0)).isEqualTo(MAX_RETRY_DELAY);
        // Nor hold back the dead-letter queue: no attempt follows the last delivery.
        assertThat(retryDelay(3, 3, Duration.ofMinutes(5), 0)).isZero();
    }

    @Test
    void aThrottledEventIsSentAgainAfterTheWaitItsDeliveryWouldGetEvenTheLastOne() {
        assertThat(UploadEventWorker.requeueDelay(1, Duration.ofSeconds(30), VISIBILITY_TIMEOUT, MAX_RETRY_DELAY, 0))
                .isEqualTo(Duration.ofSeconds(60));
        assertThat(UploadEventWorker.requeueDelay(3, Duration.ofSeconds(30), VISIBILITY_TIMEOUT, MAX_RETRY_DELAY, 0))
                .isEqualTo(Duration.ofSeconds(240));
        // SQS delays a message it is sent by at most 15 minutes, whatever max-retry-delay allows.
        assertThat(UploadEventWorker.requeueDelay(1, Duration.ofHours(2), VISIBILITY_TIMEOUT, Duration.ofHours(1), 0))
                .isEqualTo(Duration.ofMinutes(15));
    }

    @Test
    void anEventCarriesHowOftenItWasSentAgain() {
        assertThat(UploadEventWorker.throttledRequeues(Message.builder().build())).isZero();
        assertThat(UploadEventWorker.throttledRequeues(requeued("2"))).isEqualTo(2);
        // An unreadable count stops the requeues rather than risk sending the event around forever.
        assertThat(UploadEventWorker.throttledRequeues(requeued("two"))).isEqualTo(Integer.MAX_VALUE);
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

    private static Message requeued(String count) {
        return Message.builder()
                .messageAttributes(Map.of(UploadEventWorker.THROTTLED_REQUEUES,
                        MessageAttributeValue.builder().dataType("Number").stringValue(count).build()))
                .build();
    }

    private static Duration retryDelay(int receiveCount) {
        return retryDelay(receiveCount, NO_LIMIT, NO_RETRY_AFTER, 0);
    }

    private static Duration retryDelay(int receiveCount, int maxReceiveCount, Duration retryAfter, double random) {
        return UploadEventWorker.retryDelay(receiveCount, maxReceiveCount, retryAfter, VISIBILITY_TIMEOUT,
                MAX_RETRY_DELAY, random);
    }
}
