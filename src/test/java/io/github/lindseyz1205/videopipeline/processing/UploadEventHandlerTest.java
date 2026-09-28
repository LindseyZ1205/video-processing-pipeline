package io.github.lindseyz1205.videopipeline.processing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.github.lindseyz1205.videopipeline.job.JobStore;
import io.github.lindseyz1205.videopipeline.job.Lease;
import io.github.lindseyz1205.videopipeline.transcription.TranscriptionService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.util.unit.DataSize;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

/**
 * The delete-or-keep decision for every situation a message can be in. Whether a message is deleted decides whether
 * SQS retries it, so this is where "exactly one successful processing" is won or lost.
 */
class UploadEventHandlerTest {

    private static final String BUCKET = "video-uploads";
    private static final String UPLOAD_ID = "0f8fad5b-d9cb-469f-a165-70867728950e";
    private static final String KEY = "uploads/user-1/" + UPLOAD_ID + "/talk.mp4";
    private static final Instant NOW = Instant.parse("2026-09-28T12:00:05Z");
    private static final String EVENT = """
            {"Records":[{"eventName":"ObjectCreated:Put","eventTime":"2026-09-28T12:00:00.000Z",
              "s3":{"bucket":{"name":"%s"},"object":{"key":"%s"}}}]}
            """.formatted(BUCKET, KEY);
    private static final Lease LEASE = new Lease(UPLOAD_ID, "token-1", 1);

    private final JobStore jobs = mock(JobStore.class);
    private final S3Client s3 = mock(S3Client.class);
    private final TranscriptionService transcription = mock(TranscriptionService.class);
    private final SimpleMeterRegistry metrics = new SimpleMeterRegistry();
    private final UploadEventHandler handler = new UploadEventHandler(jobs, s3, transcription,
            new PipelineMetrics(metrics, Clock.fixed(NOW, ZoneOffset.UTC)), DataSize.ofMegabytes(100));

    @BeforeEach
    void uploadedObjectExists() {
        when(s3.headObject(any(HeadObjectRequest.class)))
                .thenReturn(HeadObjectResponse.builder().contentLength(1_024L).contentType("video/mp4").build());
    }

    @Test
    void deletesTheMessageOnceTheTranscriptIsStored() {
        when(jobs.tryAcquire(UPLOAD_ID, BUCKET, KEY)).thenReturn(Optional.of(LEASE));
        when(transcription.transcribe(any())).thenReturn("hello");
        when(jobs.complete(LEASE, "hello")).thenReturn(true);

        assertThat(handler.handle(EVENT)).isTrue();
        assertThat(events("processed")).isEqualTo(1);
        assertThat(transcriptions("success")).isEqualTo(1);
        assertThat(metrics.get("pipeline.events.lag").timer().totalTime(TimeUnit.SECONDS)).isEqualTo(5);
    }

    @Test
    void deletesDuplicatesOfACompletedJobWithoutProcessingAgain() {
        when(jobs.tryAcquire(UPLOAD_ID, BUCKET, KEY)).thenReturn(Optional.empty());
        when(jobs.isCompleted(UPLOAD_ID)).thenReturn(true);

        assertThat(handler.handle(EVENT)).isTrue();
        verifyNoInteractions(transcription);
        assertThat(events("duplicate")).isEqualTo(1);
        assertThat(metrics.get("pipeline.events.lag").timer().count()).isZero();
    }

    @Test
    void keepsTheMessageWhileAnotherWorkerHoldsTheLease() {
        when(jobs.tryAcquire(UPLOAD_ID, BUCKET, KEY)).thenReturn(Optional.empty());
        when(jobs.isCompleted(UPLOAD_ID)).thenReturn(false);

        assertThat(handler.handle(EVENT)).isFalse();
        verifyNoInteractions(transcription);
        assertThat(events("busy")).isEqualTo(1);
    }

    @Test
    void keepsTheMessageWhenTheLeaseWasLostBeforeCompleting() {
        when(jobs.tryAcquire(UPLOAD_ID, BUCKET, KEY)).thenReturn(Optional.of(LEASE));
        when(transcription.transcribe(any())).thenReturn("hello");
        when(jobs.complete(LEASE, "hello")).thenReturn(false);

        assertThat(handler.handle(EVENT)).isFalse();
        assertThat(events("busy")).isEqualTo(1);
    }

    @Test
    void keepsTheMessageForRetryWhenProcessingFails() {
        when(jobs.tryAcquire(UPLOAD_ID, BUCKET, KEY)).thenReturn(Optional.of(LEASE));
        when(transcription.transcribe(any())).thenThrow(new IllegalStateException("provider timed out"));

        assertThat(handler.handle(EVENT)).isFalse();
        verify(jobs).fail(eq(LEASE), contains("provider timed out"));
        assertThat(events("failed")).isEqualTo(1);
        assertThat(transcriptions("failure")).isEqualTo(1);
    }

    @Test
    void deletesTheMessageWhenTheFileCanNeverBeProcessed() {
        when(s3.headObject(any(HeadObjectRequest.class)))
                .thenReturn(HeadObjectResponse.builder().contentLength(DataSize.ofMegabytes(200).toBytes()).build());
        when(jobs.tryAcquire(UPLOAD_ID, BUCKET, KEY)).thenReturn(Optional.of(LEASE));

        assertThat(handler.handle(EVENT)).isTrue();
        verify(jobs).fail(eq(LEASE), contains("limit"));
        verifyNoInteractions(transcription);
        assertThat(events("rejected")).isEqualTo(1);
    }

    @Test
    void doesNotTreatAnUnreachableJobStoreAsADuplicate() {
        when(jobs.tryAcquire(UPLOAD_ID, BUCKET, KEY)).thenThrow(new IllegalStateException("DynamoDB unavailable"));

        assertThatThrownBy(() -> handler.handle(EVENT)).hasMessageContaining("DynamoDB unavailable");
        verifyNoInteractions(transcription);
    }

    @Test
    void deletesTheS3TestEvent() {
        assertThat(handler.handle("{\"Service\":\"Amazon S3\",\"Event\":\"s3:TestEvent\"}")).isTrue();
        verifyNoInteractions(jobs);
    }

    @Test
    void deletesEventsForObjectsThatAreNotUploads() {
        assertThat(handler.handle(EVENT.replace(KEY, "transcripts/summary.txt"))).isTrue();
        verifyNoInteractions(jobs);
        assertThat(events("ignored")).isEqualTo(1);
    }

    private double events(String outcome) {
        return metrics.get("pipeline.events").tag("outcome", outcome).counter().count();
    }

    private long transcriptions(String outcome) {
        return metrics.get("pipeline.transcription").tag("outcome", outcome).timer().count();
    }
}
