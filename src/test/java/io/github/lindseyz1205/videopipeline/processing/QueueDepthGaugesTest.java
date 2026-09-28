package io.github.lindseyz1205.videopipeline.processing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

class QueueDepthGaugesTest {

    private final SqsClient sqs = mock(SqsClient.class);
    private final SimpleMeterRegistry metrics = new SimpleMeterRegistry();
    private final QueueDepthGauges gauges =
            new QueueDepthGauges(sqs, metrics, "upload-events", "upload-events-dlq", Duration.ofSeconds(30));

    @Test
    void publishesVisibleAndInFlightCountsForBothQueues() {
        queueHas("upload-events", 4, 2);
        queueHas("upload-events-dlq", 1, 0);

        gauges.refresh();

        assertThat(depth("events", "visible")).isEqualTo(4);
        assertThat(depth("events", "in-flight")).isEqualTo(2);
        assertThat(depth("dead-letter", "visible")).isEqualTo(1);
        assertThat(depth("dead-letter", "in-flight")).isZero();
    }

    @Test
    void keepsTheLastValueWhileSqsIsUnreachable() {
        queueHas("upload-events", 4, 2);
        queueHas("upload-events-dlq", 1, 0);
        gauges.refresh();

        doThrow(SdkClientException.create("connection refused"))
                .when(sqs).getQueueAttributes(any(GetQueueAttributesRequest.class));
        gauges.refresh();

        assertThat(depth("dead-letter", "visible")).isEqualTo(1);
    }

    private void queueHas(String queueName, int visible, int inFlight) {
        String url = "https://sqs.us-east-1.amazonaws.com/000000000000/" + queueName;
        when(sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build()))
                .thenReturn(GetQueueUrlResponse.builder().queueUrl(url).build());
        when(sqs.getQueueAttributes(argThat((GetQueueAttributesRequest request) ->
                request != null && url.equals(request.queueUrl()))))
                .thenReturn(GetQueueAttributesResponse.builder()
                        .attributes(Map.of(
                                QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES, String.valueOf(visible),
                                QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE, String.valueOf(inFlight)))
                        .build());
    }

    private double depth(String queue, String state) {
        return metrics.get("pipeline.queue.messages").tags("queue", queue, "state", state).gauge().value();
    }
}
