package io.github.lindseyz1205.videopipeline.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.unit.DataSize;

/**
 * Settings under {@code pipeline.*}. Defaults live in {@code application.yml}.
 */
@ConfigurationProperties(prefix = "pipeline")
public record PipelineProperties(
        Aws aws,
        String bucket,
        Queue queue,
        Jobs jobs,
        Upload upload,
        Worker worker,
        Transcription transcription,
        Metrics metrics) {

    /**
     * @param endpoint           endpoint override for LocalStack; empty means real AWS
     * @param presignEndpoint    endpoint to put in presigned URLs, when clients reach S3 at a different address than
     *                           the service does (in docker compose: localhost vs. the LocalStack container);
     *                           empty means the same as {@code endpoint}
     * @param bootstrapResources create the bucket, queues and table on startup (LocalStack and tests only)
     */
    public record Aws(String region, String endpoint, String presignEndpoint, String accessKey, String secretKey,
            boolean bootstrapResources) {
    }

    /**
     * @param maxReceiveCount deliveries before SQS moves a message to the dead-letter queue
     */
    public record Queue(String name, String deadLetterName, int maxReceiveCount) {
    }

    /**
     * @param retention how long job records are kept before DynamoDB TTL deletes them
     */
    public record Jobs(String table, Duration retention) {
    }

    public record Upload(Duration urlTtl, DataSize maxFileSize) {
    }

    /**
     * @param visibilityTimeout how long a received message stays hidden from other pollers. It is also the lease on
     *                          the job, so when a crashed worker's message reappears the next delivery can take over.
     * @param heartbeatInterval how often a job that's still running extends both its message's visibility and its
     *                          lease. Must be shorter than the visibility timeout.
     * @param maxProcessingTime how long the heartbeat keeps extending the work on one message. Past that it gives up,
     *                          so a hung attempt's lease runs out and the next delivery takes the job over. Must be
     *                          longer than the visibility timeout.
     * @param maxRetryDelay     the longest a message waits after a failed attempt. The wait starts at one visibility
     *                          timeout and doubles with each delivery. Between the visibility timeout and 12 hours.
     */
    public record Worker(boolean enabled, int concurrency, int maxMessages, Duration waitTime, Duration visibilityTimeout,
            Duration heartbeatInterval, Duration maxProcessingTime, Duration maxRetryDelay) {
    }

    public record Transcription(String provider, OpenAi openai) {

        /**
         * @param timeout how long to wait for the answer to one request. A request that times out fails the attempt,
         *                and SQS retries it.
         */
        public record OpenAi(String apiKey, String model, String baseUrl, Duration timeout) {
        }
    }

    /**
     * @param queueDepthInterval how often the queue depth gauges are refreshed from SQS
     */
    public record Metrics(Duration queueDepthInterval) {
    }
}
