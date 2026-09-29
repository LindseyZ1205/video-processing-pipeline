package io.github.lindseyz1205.videopipeline.config;

import io.github.lindseyz1205.videopipeline.job.DynamoDbJobStore;
import io.github.lindseyz1205.videopipeline.upload.UploadKey;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TimeToLiveSpecification;
import software.amazon.awssdk.services.dynamodb.model.UpdateTimeToLiveRequest;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.FilterRule;
import software.amazon.awssdk.services.s3.model.FilterRuleName;
import software.amazon.awssdk.services.s3.model.NotificationConfiguration;
import software.amazon.awssdk.services.s3.model.NotificationConfigurationFilter;
import software.amazon.awssdk.services.s3.model.PutBucketNotificationConfigurationRequest;
import software.amazon.awssdk.services.s3.model.QueueConfiguration;
import software.amazon.awssdk.services.s3.model.S3KeyFilter;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.QueueNameExistsException;
import software.amazon.awssdk.services.sqs.model.SetQueueAttributesRequest;

/**
 * Creates the pipeline's AWS resources in LocalStack, so {@code docker compose up} plus {@code bootRun} is enough to
 * try the service and the integration tests run against the same setup. In a real AWS account they come from
 * {@code infra/terraform} instead, and CI checks that path separately.
 */
@Component
@ConditionalOnProperty(prefix = "pipeline.aws", name = "bootstrap-resources", havingValue = "true")
class LocalAwsResources implements InitializingBean {

    private static final Logger log = LoggerFactory.getLogger(LocalAwsResources.class);

    private final S3Client s3;
    private final SqsClient sqs;
    private final DynamoDbClient dynamo;
    private final PipelineProperties properties;

    LocalAwsResources(S3Client s3, SqsClient sqs, DynamoDbClient dynamo, PipelineProperties properties) {
        this.s3 = s3;
        this.sqs = sqs;
        this.dynamo = dynamo;
        this.properties = properties;
    }

    @Override
    public void afterPropertiesSet() {
        createBucket();
        String deadLetterQueueUrl = createQueue(properties.queue().deadLetterName(), Map.of());
        String redrivePolicy = "{\"deadLetterTargetArn\":\"%s\",\"maxReceiveCount\":%d}"
                .formatted(queueArn(deadLetterQueueUrl), properties.queue().maxReceiveCount());
        String queueUrl = createQueue(properties.queue().name(), Map.of(QueueAttributeName.REDRIVE_POLICY, redrivePolicy));
        sendUploadEventsTo(queueArn(queueUrl));
        createJobsTable();
        log.info("Bootstrapped bucket '{}', queue '{}' (dead-letter queue '{}') and table '{}'", properties.bucket(),
                properties.queue().name(), properties.queue().deadLetterName(), properties.jobs().table());
    }

    private void createBucket() {
        CreateBucketRequest.Builder request = CreateBucketRequest.builder().bucket(properties.bucket());
        String region = properties.aws().region();
        if (!Region.US_EAST_1.id().equals(region)) {
            request.createBucketConfiguration(CreateBucketConfiguration.builder().locationConstraint(region).build());
        }
        try {
            s3.createBucket(request.build());
        } catch (BucketAlreadyOwnedByYouException | BucketAlreadyExistsException e) {
            log.debug("Bucket {} already exists", properties.bucket());
        }
    }

    private String createQueue(String name, Map<QueueAttributeName, String> attributes) {
        try {
            return sqs.createQueue(CreateQueueRequest.builder().queueName(name).attributes(attributes).build()).queueUrl();
        } catch (QueueNameExistsException e) {
            // Same name with different attributes, e.g. a changed maxReceiveCount: update the queue in place.
            String queueUrl = sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(name).build()).queueUrl();
            if (!attributes.isEmpty()) {
                sqs.setQueueAttributes(SetQueueAttributesRequest.builder().queueUrl(queueUrl).attributes(attributes).build());
            }
            return queueUrl;
        }
    }

    private String queueArn(String queueUrl) {
        return sqs.getQueueAttributes(GetQueueAttributesRequest.builder()
                        .queueUrl(queueUrl)
                        .attributeNames(QueueAttributeName.QUEUE_ARN)
                        .build())
                .attributes()
                .get(QueueAttributeName.QUEUE_ARN);
    }

    /**
     * Only keys under the upload prefix trigger events, so anything else written to the bucket can't feed back into
     * the pipeline.
     */
    private void sendUploadEventsTo(String queueArn) {
        QueueConfiguration uploadEvents = QueueConfiguration.builder()
                .queueArn(queueArn)
                .eventsWithStrings("s3:ObjectCreated:*")
                .filter(NotificationConfigurationFilter.builder()
                        .key(S3KeyFilter.builder()
                                .filterRules(FilterRule.builder().name(FilterRuleName.PREFIX).value(UploadKey.PREFIX).build())
                                .build())
                        .build())
                .build();
        s3.putBucketNotificationConfiguration(PutBucketNotificationConfigurationRequest.builder()
                .bucket(properties.bucket())
                .notificationConfiguration(NotificationConfiguration.builder().queueConfigurations(uploadEvents).build())
                .build());
    }

    private void createJobsTable() {
        String table = properties.jobs().table();
        try {
            dynamo.createTable(CreateTableRequest.builder()
                    .tableName(table)
                    .attributeDefinitions(AttributeDefinition.builder()
                            .attributeName(DynamoDbJobStore.JOB_ID)
                            .attributeType(ScalarAttributeType.S)
                            .build())
                    .keySchema(KeySchemaElement.builder()
                            .attributeName(DynamoDbJobStore.JOB_ID)
                            .keyType(KeyType.HASH)
                            .build())
                    .billingMode(BillingMode.PAY_PER_REQUEST)
                    .build());
        } catch (ResourceInUseException e) {
            log.debug("Table {} already exists", table);
            return;
        }
        try (DynamoDbWaiter waiter = dynamo.waiter()) {
            waiter.waitUntilTableExists(DescribeTableRequest.builder().tableName(table).build());
        }
        dynamo.updateTimeToLive(UpdateTimeToLiveRequest.builder()
                .tableName(table)
                .timeToLiveSpecification(TimeToLiveSpecification.builder()
                        .enabled(true)
                        .attributeName(DynamoDbJobStore.EXPIRES_AT)
                        .build())
                .build());
    }
}
