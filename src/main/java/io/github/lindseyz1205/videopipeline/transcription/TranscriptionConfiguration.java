package io.github.lindseyz1205.videopipeline.transcription;

import io.github.lindseyz1205.videopipeline.config.PipelineProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Chooses the provider from {@code pipeline.transcription.provider}.
 */
@Configuration(proxyBeanMethods = false)
public class TranscriptionConfiguration {

    @Bean
    @ConditionalOnProperty(name = "pipeline.transcription.provider", havingValue = "fake", matchIfMissing = true)
    public TranscriptionService fakeTranscriptionService() {
        return new FakeTranscriptionService();
    }

    @Bean
    @ConditionalOnProperty(name = "pipeline.transcription.provider", havingValue = "openai")
    public TranscriptionService openAiTranscriptionService(RestClient.Builder restClientBuilder, S3Client s3,
            PipelineProperties properties) {
        return new OpenAiTranscriptionService(restClientBuilder, s3, properties.transcription().openai());
    }
}
