package io.github.lindseyz1205.videopipeline.config;

import java.net.URI;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;

/**
 * AWS SDK clients. With {@code pipeline.aws.endpoint} set (LocalStack) every client points there and S3 uses
 * path-style URLs; otherwise the SDK's normal endpoint and credential resolution applies.
 */
@Configuration(proxyBeanMethods = false)
public class AwsConfiguration {

    private final PipelineProperties.Aws aws;

    public AwsConfiguration(PipelineProperties properties) {
        this.aws = properties.aws();
    }

    @Bean
    public AwsCredentialsProvider awsCredentialsProvider() {
        if (StringUtils.hasText(aws.accessKey()) && StringUtils.hasText(aws.secretKey())) {
            return StaticCredentialsProvider.create(AwsBasicCredentials.create(aws.accessKey(), aws.secretKey()));
        }
        return DefaultCredentialsProvider.create();
    }

    @Bean
    public S3Client s3Client(AwsCredentialsProvider credentials) {
        S3ClientBuilder builder = S3Client.builder().region(region()).credentialsProvider(credentials);
        if (hasEndpointOverride()) {
            builder.endpointOverride(endpoint()).forcePathStyle(true);
        }
        return builder.build();
    }

    @Bean
    public S3Presigner s3Presigner(AwsCredentialsProvider credentials) {
        S3Presigner.Builder builder = S3Presigner.builder().region(region()).credentialsProvider(credentials);
        if (hasEndpointOverride()) {
            URI presignEndpoint = StringUtils.hasText(aws.presignEndpoint()) ? URI.create(aws.presignEndpoint()) : endpoint();
            builder.endpointOverride(presignEndpoint)
                    .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build());
        }
        return builder.build();
    }

    @Bean
    public SqsClient sqsClient(AwsCredentialsProvider credentials) {
        SqsClientBuilder builder = SqsClient.builder().region(region()).credentialsProvider(credentials);
        if (hasEndpointOverride()) {
            builder.endpointOverride(endpoint());
        }
        return builder.build();
    }

    @Bean
    public DynamoDbClient dynamoDbClient(AwsCredentialsProvider credentials) {
        DynamoDbClientBuilder builder = DynamoDbClient.builder().region(region()).credentialsProvider(credentials);
        if (hasEndpointOverride()) {
            builder.endpointOverride(endpoint());
        }
        return builder.build();
    }

    private Region region() {
        return Region.of(aws.region());
    }

    private boolean hasEndpointOverride() {
        return StringUtils.hasText(aws.endpoint());
    }

    private URI endpoint() {
        return URI.create(aws.endpoint());
    }
}
