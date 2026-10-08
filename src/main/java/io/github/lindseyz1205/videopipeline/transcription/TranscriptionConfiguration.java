package io.github.lindseyz1205.videopipeline.transcription;

import io.github.lindseyz1205.videopipeline.config.PipelineProperties;
import java.net.http.HttpClient;
import java.time.Duration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Chooses the provider from {@code pipeline.transcription.provider}.
 */
@Configuration(proxyBeanMethods = false)
public class TranscriptionConfiguration {

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);

    @Bean
    @ConditionalOnProperty(name = "pipeline.transcription.provider", havingValue = "fake", matchIfMissing = true)
    public TranscriptionService fakeTranscriptionService() {
        return new FakeTranscriptionService();
    }

    @Bean
    @ConditionalOnProperty(name = "pipeline.transcription.provider", havingValue = "openai")
    public TranscriptionService openAiTranscriptionService(RestClient.Builder restClientBuilder, S3Client s3,
            PipelineProperties properties) {
        return openAi(restClientBuilder, s3, properties.transcription().openai());
    }

    /**
     * Gives the HTTP client explicit timeouts. Without them, a request that never gets an answer would block its worker
     * thread for good.
     */
    static OpenAiTranscriptionService openAi(RestClient.Builder restClientBuilder, S3Client s3,
            PipelineProperties.Transcription.OpenAi settings) {
        JdkClientHttpRequestFactory requestFactory =
                new JdkClientHttpRequestFactory(HttpClient.newBuilder().connectTimeout(CONNECT_TIMEOUT).build());
        requestFactory.setReadTimeout(settings.timeout());
        return new OpenAiTranscriptionService(restClientBuilder.requestFactory(requestFactory), s3, settings);
    }
}
