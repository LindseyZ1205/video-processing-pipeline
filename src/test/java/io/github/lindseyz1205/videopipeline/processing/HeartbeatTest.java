package io.github.lindseyz1205.videopipeline.processing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.github.lindseyz1205.videopipeline.job.JobStore;
import io.github.lindseyz1205.videopipeline.job.Lease;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;

/**
 * The tests call {@code beat()} by hand instead of waiting for the scheduler, so they don't depend on timing.
 */
class HeartbeatTest {

    private static final Lease LEASE = new Lease("0f8fad5b-d9cb-469f-a165-70867728950e", "token-1", 1);

    private final SqsClient sqs = mock(SqsClient.class);
    private final JobStore jobs = mock(JobStore.class);
    private final ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    private final ScheduledFuture<?> beats = mock(ScheduledFuture.class);
    private final Heartbeat heartbeat = new Heartbeat(sqs, jobs, scheduler,
            "https://sqs.us-east-1.amazonaws.com/000000000000/video-upload-events", "receipt-1",
            Duration.ofSeconds(60), Duration.ofSeconds(20));

    @BeforeEach
    void leasesCanBeExtended() {
        doReturn(beats).when(scheduler).scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
        when(jobs.extendLease(LEASE)).thenReturn(true);
    }

    @Test
    void doesNothingUntilAJobIsLeased() {
        heartbeat.beat();

        verifyNoInteractions(scheduler, sqs, jobs);
    }

    @Test
    void startsBeatingAtTheIntervalOnceAJobIsLeased() {
        heartbeat.keepAlive(LEASE);

        verify(scheduler).scheduleAtFixedRate(any(Runnable.class), eq(20_000L), eq(20_000L), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void everyBeatExtendsTheMessageAndTheLease() {
        heartbeat.keepAlive(LEASE);

        heartbeat.beat();
        heartbeat.beat();

        ArgumentCaptor<ChangeMessageVisibilityRequest> request =
                ArgumentCaptor.forClass(ChangeMessageVisibilityRequest.class);
        verify(sqs, times(2)).changeMessageVisibility(request.capture());
        assertThat(request.getValue().receiptHandle()).isEqualTo("receipt-1");
        assertThat(request.getValue().visibilityTimeout()).isEqualTo(60);
        verify(jobs, times(2)).extendLease(LEASE);
    }

    @Test
    void stopsOnceTheWorkIsDone() {
        heartbeat.keepAlive(LEASE);
        heartbeat.release(LEASE);

        heartbeat.beat();
        heartbeat.close();

        verify(beats).cancel(false);
        verifyNoInteractions(sqs);
        verify(jobs, never()).extendLease(any());
    }

    @Test
    void stopsWhenTheLeaseWasLost() {
        when(jobs.extendLease(LEASE)).thenReturn(false);
        heartbeat.keepAlive(LEASE);

        heartbeat.beat();
        heartbeat.beat();

        verify(beats).cancel(false);
        verify(sqs, times(1)).changeMessageVisibility(any(ChangeMessageVisibilityRequest.class));
        verify(jobs, times(1)).extendLease(LEASE);
    }

    @Test
    void cannotBeRestartedAfterClosing() {
        heartbeat.close();

        heartbeat.keepAlive(LEASE);
        heartbeat.beat();

        verifyNoInteractions(scheduler, sqs, jobs);
    }
}
