package io.github.lindseyz1205.videopipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.lindseyz1205.videopipeline.config.PipelineProperties;
import io.github.lindseyz1205.videopipeline.job.JobStatus;
import io.github.lindseyz1205.videopipeline.transcription.StoredObject;
import io.github.lindseyz1205.videopipeline.transcription.TranscriptionService;
import io.github.lindseyz1205.videopipeline.upload.CreateUploadRequest;
import io.github.lindseyz1205.videopipeline.upload.CreateUploadResponse;
import io.github.lindseyz1205.videopipeline.upload.UploadStatusResponse;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

/**
 * The whole pipeline against LocalStack: presigned upload, S3 event notification, SQS, worker, DynamoDB. The
 * visibility timeout and the max processing time are cut to seconds, so retries, take-overs and the dead-letter queue
 * show up quickly.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "pipeline.aws.bootstrap-resources=true",
                "pipeline.worker.visibility-timeout=3s",
                "pipeline.worker.heartbeat-interval=1s",
                "pipeline.worker.max-processing-time=10s",
                "pipeline.worker.wait-time=1s",
                "pipeline.queue.max-receive-count=3",
                "pipeline.metrics.queue-depth-interval=1s"
        })
@AutoConfigureObservability // tests skip metrics export by default; this turns on /actuator/prometheus
class PipelineIntegrationTest {

    private static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:4.6.0"));

    static {
        LOCALSTACK.start();
    }

    @DynamicPropertySource
    static void pointAwsAtLocalStack(DynamicPropertyRegistry registry) {
        registry.add("pipeline.aws.endpoint", () -> LOCALSTACK.getEndpoint().toString());
        registry.add("pipeline.aws.region", LOCALSTACK::getRegion);
        registry.add("pipeline.aws.access-key", LOCALSTACK::getAccessKey);
        registry.add("pipeline.aws.secret-key", LOCALSTACK::getSecretKey);
    }

    private static final byte[] FILE_CONTENT = "not really a video, but S3 does not mind".getBytes(StandardCharsets.UTF_8);

    @Autowired
    private TestRestTemplate api;

    @Autowired
    private ScriptedTranscriptionService transcriber;

    @Autowired
    private SqsClient sqs;

    @Autowired
    private PipelineProperties properties;

    private final HttpClient http = HttpClient.newHttpClient();

    @Test
    void uploadedFileIsTranscribed() throws Exception {
        CreateUploadResponse upload = createUpload("team-sync.mp4");
        assertThat(status(upload.uploadId()).status()).isEqualTo(JobStatus.PENDING_UPLOAD);

        putFile(upload);

        UploadStatusResponse done = awaitStatus(upload.uploadId(), JobStatus.COMPLETED);
        assertThat(done.transcript()).isEqualTo("transcript of " + upload.objectKey());
        assertThat(done.attempts()).isEqualTo(1);
    }

    @Test
    void duplicateDeliveriesAreProcessedOnce() throws Exception {
        CreateUploadResponse upload = createUpload("duplicates.mp4");
        putFile(upload);
        // S3 notifications are at-least-once. Deliver the same event twice more, as S3 occasionally does.
        sendUploadEvent(upload.objectKey());
        sendUploadEvent(upload.objectKey());

        awaitStatus(upload.uploadId(), JobStatus.COMPLETED);
        await().atMost(Duration.ofSeconds(30)).until(this::uploadQueueIsEmpty);

        assertThat(transcriber.calls(upload.objectKey())).isEqualTo(1);
        assertThat(status(upload.uploadId()).attempts()).isEqualTo(1);
    }

    @Test
    void failedAttemptIsRetried() throws Exception {
        CreateUploadResponse upload = createUpload("flaky.mp4");
        transcriber.failNextCalls(upload.objectKey(), 1);

        putFile(upload);

        UploadStatusResponse done = awaitStatus(upload.uploadId(), JobStatus.COMPLETED);
        assertThat(done.attempts()).isEqualTo(2);
        assertThat(transcriber.calls(upload.objectKey())).isEqualTo(2);
    }

    @Test
    void transcriptionLongerThanTheVisibilityTimeoutIsProcessedOnce() throws Exception {
        CreateUploadResponse upload = createUpload("all-hands.mp4");
        // More than two 3 s visibility timeouts. Without the heartbeat the message would reappear meanwhile, and once
        // the lease ran out the second poller would take the job over and transcribe the file again.
        transcriber.delay(upload.objectKey(), Duration.ofSeconds(8));

        putFile(upload);

        UploadStatusResponse done = awaitStatus(upload.uploadId(), JobStatus.COMPLETED);
        await().atMost(Duration.ofSeconds(30)).until(this::uploadQueueIsEmpty);
        assertThat(done.attempts()).isEqualTo(1);
        assertThat(transcriber.calls(upload.objectKey())).isEqualTo(1);

        // An 8 s transcription under a 3 s lease needs at least two extensions. Finishing normally isn't a lost lease.
        String scrape = api.getForObject("/actuator/prometheus", String.class);
        assertThat(sum(scrape, "pipeline_lease_extensions_total", "outcome=\"extended\"")).isGreaterThanOrEqualTo(2);
        assertThat(sum(scrape, "pipeline_lease_extensions_total", "outcome=\"lost\"")).isZero();
    }

    @Test
    void transcriptionThatHangsIsTakenOverAfterTheMaxProcessingTime() throws Exception {
        CreateUploadResponse upload = createUpload("frozen.mp4");
        // The first attempt never returns. After 10 s its heartbeat gives up, its lease runs out, and the next
        // delivery transcribes the file instead.
        transcriber.hangFirstCall(upload.objectKey());
        try {
            putFile(upload);

            UploadStatusResponse done = awaitStatus(upload.uploadId(), JobStatus.COMPLETED);
            assertThat(done.attempts()).isEqualTo(2);
            assertThat(transcriber.calls(upload.objectKey())).isEqualTo(2);
            String scrape = api.getForObject("/actuator/prometheus", String.class);
            assertThat(sum(scrape, "pipeline_lease_extensions_total", "outcome=\"abandoned\""))
                    .isGreaterThanOrEqualTo(1);
        } finally {
            transcriber.release(upload.objectKey());
        }
    }

    @Test
    void eventThatKeepsFailingEndsUpInTheDeadLetterQueue() throws Exception {
        CreateUploadResponse upload = createUpload("broken.mp4");
        transcriber.failNextCalls(upload.objectKey(), Integer.MAX_VALUE);

        putFile(upload);

        String deadLetterQueueUrl = queueUrl(properties.queue().deadLetterName());
        await().atMost(Duration.ofSeconds(60)).until(
                () -> sqs.receiveMessage(r -> r.queueUrl(deadLetterQueueUrl).waitTimeSeconds(1).maxNumberOfMessages(10))
                        .messages(),
                messages -> messages.stream().map(Message::body).anyMatch(body -> body.contains(upload.objectKey())));

        int maxReceiveCount = properties.queue().maxReceiveCount();
        UploadStatusResponse status = status(upload.uploadId());
        assertThat(status.status()).isEqualTo(JobStatus.FAILED);
        assertThat(status.attempts()).isEqualTo(maxReceiveCount);
        assertThat(status.error()).contains("simulated transcription failure");
        assertThat(transcriber.calls(upload.objectKey())).isEqualTo(maxReceiveCount);
    }

    @Test
    void exposesPipelineMetricsForPrometheus() throws Exception {
        CreateUploadResponse upload = createUpload("metrics.mp4");
        putFile(upload);
        awaitStatus(upload.uploadId(), JobStatus.COMPLETED);

        String scrape = api.getForObject("/actuator/prometheus", String.class);

        assertThat(sum(scrape, "pipeline_events_total", "outcome=\"processed\"")).isGreaterThanOrEqualTo(1);
        assertThat(sum(scrape, "pipeline_transcription_seconds_count", "outcome=\"success\"")).isGreaterThanOrEqualTo(1);
        assertThat(sum(scrape, "pipeline_events_lag_seconds_count", "")).isGreaterThanOrEqualTo(1);
        assertThat(scrape).contains("pipeline_queue_messages{", "queue=\"dead-letter\"", "state=\"in-flight\"");
    }

    @Test
    void documentsTheApiWithOpenApi() {
        String spec = api.getForObject("/v3/api-docs", String.class);

        assertThat(spec).contains("Video processing pipeline API", "\"/api/uploads\"", "\"/api/uploads/{uploadId}\"");
        assertThat(spec).doesNotContain("/actuator");
        assertThat(api.getForEntity("/swagger-ui/index.html", String.class).getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    void refusesFilesThatCannotBeTranscribed() {
        var request = new CreateUploadRequest("test-user", "notes.pdf", "application/pdf", 1_000);

        ResponseEntity<String> response = api.postForEntity("/api/uploads", request, String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    private CreateUploadResponse createUpload(String fileName) {
        var request = new CreateUploadRequest("test-user", fileName, "video/mp4", FILE_CONTENT.length);
        ResponseEntity<CreateUploadResponse> response =
                api.postForEntity("/api/uploads", request, CreateUploadResponse.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        return response.getBody();
    }

    /** Uploads the way a browser would: straight to S3 with the presigned URL and the required headers. */
    private void putFile(CreateUploadResponse upload) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(upload.uploadUrl()))
                .method(upload.method(), HttpRequest.BodyPublishers.ofByteArray(FILE_CONTENT));
        upload.headers().forEach(request::header);

        HttpResponse<String> response = http.send(request.build(), HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
    }

    private UploadStatusResponse status(String uploadId) {
        ResponseEntity<UploadStatusResponse> response =
                api.getForEntity("/api/uploads/{id}", UploadStatusResponse.class, uploadId);
        return response.getStatusCode().is2xxSuccessful() ? response.getBody() : null;
    }

    private UploadStatusResponse awaitStatus(String uploadId, JobStatus expected) {
        return await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> status(uploadId), status -> status != null && status.status() == expected);
    }

    private void sendUploadEvent(String objectKey) {
        String event = """
                {"Records":[{"eventSource":"aws:s3","eventName":"ObjectCreated:Put",
                  "s3":{"bucket":{"name":"%s"},"object":{"key":"%s","size":%d}}}]}
                """.formatted(properties.bucket(), objectKey, FILE_CONTENT.length);
        sqs.sendMessage(r -> r.queueUrl(queueUrl(properties.queue().name())).messageBody(event));
    }

    private boolean uploadQueueIsEmpty() {
        Map<QueueAttributeName, String> attributes = sqs.getQueueAttributes(r -> r
                        .queueUrl(queueUrl(properties.queue().name()))
                        .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES,
                                QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE))
                .attributes();
        return "0".equals(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES))
                && "0".equals(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE));
    }

    private String queueUrl(String queueName) {
        return sqs.getQueueUrl(r -> r.queueName(queueName)).queueUrl();
    }

    /** Adds up the samples of {@code metric} whose labels contain {@code label}, from Prometheus text format. */
    private static double sum(String scrape, String metric, String label) {
        return scrape.lines()
                .filter(line -> line.startsWith(metric + "{") && line.contains(label))
                .mapToDouble(line -> Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1)))
                .sum();
    }

    @TestConfiguration
    static class ScriptedTranscriptionConfiguration {

        @Bean
        @Primary
        ScriptedTranscriptionService scriptedTranscriptionService() {
            return new ScriptedTranscriptionService();
        }
    }

    /** Stands in for a real provider: counts calls per file, and can be told to fail, to be slow or to hang. */
    static class ScriptedTranscriptionService implements TranscriptionService {

        private final Map<String, AtomicInteger> calls = new ConcurrentHashMap<>();
        private final Map<String, Integer> failuresLeft = new ConcurrentHashMap<>();
        private final Map<String, Duration> delays = new ConcurrentHashMap<>();
        private final Map<String, CountDownLatch> hangs = new ConcurrentHashMap<>();

        void failNextCalls(String objectKey, int times) {
            failuresLeft.put(objectKey, times);
        }

        void delay(String objectKey, Duration delay) {
            delays.put(objectKey, delay);
        }

        /** The first call for the file blocks until {@link #release} is called, like a request that never returns. */
        void hangFirstCall(String objectKey) {
            hangs.put(objectKey, new CountDownLatch(1));
        }

        void release(String objectKey) {
            CountDownLatch hang = hangs.get(objectKey);
            if (hang != null) {
                hang.countDown();
            }
        }

        int calls(String objectKey) {
            AtomicInteger count = calls.get(objectKey);
            return count == null ? 0 : count.get();
        }

        @Override
        public String transcribe(StoredObject object) {
            int call = calls.computeIfAbsent(object.key(), key -> new AtomicInteger()).incrementAndGet();
            CountDownLatch hang = hangs.get(object.key());
            if (hang != null && call == 1) {
                try {
                    hang.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("interrupted while simulating a hung transcription", e);
                }
            }
            Integer left = failuresLeft.computeIfPresent(object.key(), (key, remaining) -> remaining - 1);
            if (left != null && left >= 0) {
                throw new IllegalStateException("simulated transcription failure");
            }
            Duration delay = delays.get(object.key());
            if (delay != null) {
                try {
                    Thread.sleep(delay.toMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("interrupted while simulating a slow transcription", e);
                }
            }
            return "transcript of " + object.key();
        }
    }
}
