package io.github.lindseyz1205.videopipeline.job;

import io.github.lindseyz1205.videopipeline.config.PipelineProperties;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

/**
 * {@link JobStore} backed by a single DynamoDB table keyed by {@code jobId} (the upload ID).
 *
 * <p>Every state change is one conditional write. DynamoDB checks the condition and applies the update atomically, so
 * two workers racing on duplicate deliveries of the same event cannot both win.
 */
@Component
public class DynamoDbJobStore implements JobStore {

    public static final String JOB_ID = "jobId";
    /** Epoch seconds; the table's TTL attribute. */
    public static final String EXPIRES_AT = "expiresAt";

    private static final String STATUS = "status";
    private static final String ATTEMPTS = "attempts";
    private static final String BUCKET = "bucket";
    private static final String OBJECT_KEY = "objectKey";
    private static final String LEASE_TOKEN = "leaseToken";
    private static final String LEASE_EXPIRES_AT = "leaseExpiresAt";
    private static final String TRANSCRIPT = "transcript";
    private static final String LAST_ERROR = "lastError";
    private static final String CREATED_AT = "createdAt";
    private static final String UPDATED_AT = "updatedAt";

    private static final int MAX_ERROR_LENGTH = 1_000;

    private static final Logger log = LoggerFactory.getLogger(DynamoDbJobStore.class);

    private final DynamoDbClient dynamo;
    private final String table;
    private final Duration leaseDuration;
    private final Duration retention;
    private final Clock clock;

    public DynamoDbJobStore(DynamoDbClient dynamo, PipelineProperties properties, Clock clock) {
        this.dynamo = dynamo;
        this.table = properties.jobs().table();
        this.leaseDuration = properties.worker().visibilityTimeout();
        this.retention = properties.jobs().retention();
        this.clock = clock;
    }

    @Override
    public void createPending(String jobId, String bucket, String objectKey) {
        Instant now = clock.instant();
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(JOB_ID, s(jobId));
        item.put(STATUS, s(JobStatus.PENDING_UPLOAD.name()));
        item.put(ATTEMPTS, n(0));
        item.put(BUCKET, s(bucket));
        item.put(OBJECT_KEY, s(objectKey));
        item.put(CREATED_AT, s(now.toString()));
        item.put(UPDATED_AT, s(now.toString()));
        item.put(EXPIRES_AT, n(now.plus(retention).getEpochSecond()));

        dynamo.putItem(PutItemRequest.builder()
                .tableName(table)
                .item(item)
                .conditionExpression("attribute_not_exists(#jobId)")
                .expressionAttributeNames(names(JOB_ID))
                .build());
    }

    @Override
    public Optional<Lease> tryAcquire(String jobId, String bucket, String objectKey) {
        Instant now = clock.instant();
        String token = UUID.randomUUID().toString();

        Map<String, AttributeValue> values = new HashMap<>();
        values.put(":processing", s(JobStatus.PROCESSING.name()));
        values.put(":pending", s(JobStatus.PENDING_UPLOAD.name()));
        values.put(":failed", s(JobStatus.FAILED.name()));
        values.put(":nowMillis", n(now.toEpochMilli()));
        values.put(":token", s(token));
        values.put(":leaseExpiresAt", n(now.plus(leaseDuration).toEpochMilli()));
        values.put(":zero", n(0));
        values.put(":one", n(1));
        values.put(":now", s(now.toString()));
        values.put(":bucket", s(bucket));
        values.put(":objectKey", s(objectKey));
        values.put(":expiresAt", n(now.plus(retention).getEpochSecond()));

        try {
            UpdateItemResponse response = dynamo.updateItem(UpdateItemRequest.builder()
                    .tableName(table)
                    .key(key(jobId))
                    // Claimable: brand new (event arrived without a pending record), waiting for its first attempt,
                    // failed (this is a retry), or stuck in PROCESSING because its worker died and the lease ran out.
                    .conditionExpression("attribute_not_exists(#jobId)"
                            + " OR #status IN (:pending, :failed)"
                            + " OR (#status = :processing AND #leaseExpiresAt < :nowMillis)")
                    .updateExpression("SET #status = :processing, #leaseToken = :token, #leaseExpiresAt = :leaseExpiresAt,"
                            + " #attempts = if_not_exists(#attempts, :zero) + :one, #updatedAt = :now,"
                            + " #bucket = if_not_exists(#bucket, :bucket),"
                            + " #objectKey = if_not_exists(#objectKey, :objectKey),"
                            + " #createdAt = if_not_exists(#createdAt, :now),"
                            + " #expiresAt = if_not_exists(#expiresAt, :expiresAt)")
                    .expressionAttributeNames(names(JOB_ID, STATUS, LEASE_TOKEN, LEASE_EXPIRES_AT, ATTEMPTS, UPDATED_AT,
                            BUCKET, OBJECT_KEY, CREATED_AT, EXPIRES_AT))
                    .expressionAttributeValues(values)
                    .returnValues(ReturnValue.UPDATED_NEW)
                    .build());
            int attempt = Integer.parseInt(response.attributes().get(ATTEMPTS).n());
            return Optional.of(new Lease(jobId, token, attempt));
        } catch (ConditionalCheckFailedException e) {
            return Optional.empty();
        }
    }

    @Override
    public boolean complete(Lease lease, String transcript) {
        Map<String, AttributeValue> values = new HashMap<>();
        values.put(":token", s(lease.token()));
        values.put(":completed", s(JobStatus.COMPLETED.name()));
        values.put(":transcript", s(Objects.requireNonNullElse(transcript, "")));
        values.put(":now", s(clock.instant().toString()));

        return updateUnderLease(lease,
                "SET #status = :completed, #transcript = :transcript, #updatedAt = :now"
                        + " REMOVE #leaseToken, #leaseExpiresAt, #lastError",
                names(LEASE_TOKEN, STATUS, TRANSCRIPT, UPDATED_AT, LEASE_EXPIRES_AT, LAST_ERROR),
                values);
    }

    @Override
    public boolean fail(Lease lease, String error) {
        Map<String, AttributeValue> values = new HashMap<>();
        values.put(":token", s(lease.token()));
        values.put(":failed", s(JobStatus.FAILED.name()));
        values.put(":error", s(truncate(error)));
        values.put(":now", s(clock.instant().toString()));

        return updateUnderLease(lease,
                "SET #status = :failed, #lastError = :error, #updatedAt = :now REMOVE #leaseToken, #leaseExpiresAt",
                names(LEASE_TOKEN, STATUS, LAST_ERROR, UPDATED_AT, LEASE_EXPIRES_AT),
                values);
    }

    /** Applies an update only while {@code lease} is still the current one. */
    private boolean updateUnderLease(Lease lease, String updateExpression, Map<String, String> names,
            Map<String, AttributeValue> values) {
        try {
            dynamo.updateItem(UpdateItemRequest.builder()
                    .tableName(table)
                    .key(key(lease.jobId()))
                    .conditionExpression("#leaseToken = :token")
                    .updateExpression(updateExpression)
                    .expressionAttributeNames(names)
                    .expressionAttributeValues(values)
                    .build());
            return true;
        } catch (ConditionalCheckFailedException e) {
            log.warn("Lost the lease on job {} (attempt {}); another worker has taken it over",
                    lease.jobId(), lease.attempt());
            return false;
        }
    }

    @Override
    public boolean isCompleted(String jobId) {
        GetItemResponse response = dynamo.getItem(GetItemRequest.builder()
                .tableName(table)
                .key(key(jobId))
                .consistentRead(true)
                .projectionExpression("#status")
                .expressionAttributeNames(names(STATUS))
                .build());
        return response.hasItem() && JobStatus.COMPLETED.name().equals(string(response.item(), STATUS));
    }

    @Override
    public Optional<Job> find(String jobId) {
        GetItemResponse response = dynamo.getItem(GetItemRequest.builder()
                .tableName(table)
                .key(key(jobId))
                .consistentRead(true)
                .build());
        if (!response.hasItem() || response.item().isEmpty()) {
            return Optional.empty();
        }
        Map<String, AttributeValue> item = response.item();
        return Optional.of(new Job(
                jobId,
                JobStatus.valueOf(string(item, STATUS)),
                number(item, ATTEMPTS),
                string(item, OBJECT_KEY),
                string(item, TRANSCRIPT),
                string(item, LAST_ERROR),
                Instant.parse(string(item, UPDATED_AT))));
    }

    private static Map<String, AttributeValue> key(String jobId) {
        return Map.of(JOB_ID, s(jobId));
    }

    /** Placeholders {@code #name -> name} for every attribute, so reserved words such as "status" are never an issue. */
    private static Map<String, String> names(String... attributes) {
        Map<String, String> names = new HashMap<>();
        for (String attribute : attributes) {
            names.put("#" + attribute, attribute);
        }
        return names;
    }

    private static AttributeValue s(String value) {
        return AttributeValue.builder().s(value).build();
    }

    private static AttributeValue n(long value) {
        return AttributeValue.builder().n(Long.toString(value)).build();
    }

    private static String string(Map<String, AttributeValue> item, String name) {
        AttributeValue value = item.get(name);
        return value == null ? null : value.s();
    }

    private static int number(Map<String, AttributeValue> item, String name) {
        AttributeValue value = item.get(name);
        return value == null ? 0 : Integer.parseInt(value.n());
    }

    private static String truncate(String error) {
        if (error == null || error.isBlank()) {
            return "unknown error";
        }
        return error.length() <= MAX_ERROR_LENGTH ? error : error.substring(0, MAX_ERROR_LENGTH);
    }
}
