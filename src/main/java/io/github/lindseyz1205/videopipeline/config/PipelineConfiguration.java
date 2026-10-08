package io.github.lindseyz1205.videopipeline.config;

import io.github.lindseyz1205.videopipeline.job.JobStore;
import io.github.lindseyz1205.videopipeline.processing.PipelineMetrics;
import io.github.lindseyz1205.videopipeline.processing.QueueDepthGauges;
import io.github.lindseyz1205.videopipeline.processing.UploadEventHandler;
import io.github.lindseyz1205.videopipeline.processing.UploadEventWorker;
import io.github.lindseyz1205.videopipeline.transcription.TranscriptionService;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

@Configuration(proxyBeanMethods = false)
public class PipelineConfiguration {

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    public PipelineMetrics pipelineMetrics(MeterRegistry registry, Clock clock) {
        return new PipelineMetrics(registry, clock);
    }

    @Bean
    public QueueDepthGauges queueDepthGauges(SqsClient sqs, MeterRegistry registry, PipelineProperties properties) {
        return new QueueDepthGauges(sqs, registry, properties.queue().name(), properties.queue().deadLetterName(),
                properties.metrics().queueDepthInterval());
    }

    @Bean
    public UploadEventHandler uploadEventHandler(JobStore jobs, S3Client s3, TranscriptionService transcription,
            PipelineMetrics metrics, PipelineProperties properties) {
        return new UploadEventHandler(jobs, s3, transcription, metrics, properties.upload().maxFileSize());
    }

    @Bean
    public UploadEventWorker uploadEventWorker(SqsClient sqs, UploadEventHandler handler, JobStore jobs,
            PipelineMetrics metrics, Clock clock, PipelineProperties properties) {
        return new UploadEventWorker(sqs, handler, jobs, metrics, clock, properties.queue().name(),
                properties.worker());
    }
}
