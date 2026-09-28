package io.github.lindseyz1205.videopipeline.config;

import io.github.lindseyz1205.videopipeline.job.JobStore;
import io.github.lindseyz1205.videopipeline.processing.UploadEventHandler;
import io.github.lindseyz1205.videopipeline.processing.UploadEventWorker;
import io.github.lindseyz1205.videopipeline.transcription.TranscriptionService;
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
    public UploadEventHandler uploadEventHandler(JobStore jobs, S3Client s3, TranscriptionService transcription,
            PipelineProperties properties) {
        return new UploadEventHandler(jobs, s3, transcription, properties.upload().maxFileSize());
    }

    @Bean
    public UploadEventWorker uploadEventWorker(SqsClient sqs, UploadEventHandler handler, PipelineProperties properties) {
        return new UploadEventWorker(sqs, handler, properties.queue().name(), properties.worker());
    }
}
