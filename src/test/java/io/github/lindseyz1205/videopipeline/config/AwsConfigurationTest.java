package io.github.lindseyz1205.videopipeline.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

class AwsConfigurationTest {

    @Test
    void presignedUrlsPointAtThePresignEndpointWhenOneIsSet() {
        String url = presignedUploadUrl(
                new PipelineProperties.Aws("us-east-1", "http://localstack:4566", "http://localhost:4566", "test", "test", false));

        assertThat(url).startsWith("http://localhost:4566/video-uploads/uploads/");
    }

    @Test
    void presignedUrlsPointAtTheServiceEndpointOtherwise() {
        String url = presignedUploadUrl(
                new PipelineProperties.Aws("us-east-1", "http://localstack:4566", null, "test", "test", false));

        assertThat(url).startsWith("http://localstack:4566/video-uploads/uploads/");
    }

    private static String presignedUploadUrl(PipelineProperties.Aws aws) {
        PipelineProperties properties = new PipelineProperties(aws, null, null, null, null, null, null, null);
        AwsConfiguration configuration = new AwsConfiguration(properties);
        try (S3Presigner presigner = configuration.s3Presigner(
                StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))) {
            return presigner.presignPutObject(PutObjectPresignRequest.builder()
                            .signatureDuration(Duration.ofMinutes(15))
                            .putObjectRequest(PutObjectRequest.builder()
                                    .bucket("video-uploads")
                                    .key("uploads/user-1/0f8fad5b-d9cb-469f-a165-70867728950e/talk.mp4")
                                    .build())
                            .build())
                    .url()
                    .toString();
        }
    }
}
