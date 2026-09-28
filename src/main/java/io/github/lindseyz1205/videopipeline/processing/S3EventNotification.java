package io.github.lindseyz1205.videopipeline.processing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * The parts of an S3 event notification the worker needs. S3 puts one of these JSON documents in each SQS message
 * body. It also sends a one-off {@code s3:TestEvent} when the notification is first configured.
 *
 * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html">Event
 *      message structure</a>
 */
public record S3EventNotification(List<EventRecord> records, String event) {

    private static final ObjectMapper JSON = new ObjectMapper();

    /**
     * @throws IllegalArgumentException if the body is not a JSON object
     */
    public static S3EventNotification parse(String messageBody) {
        JsonNode root;
        try {
            root = JSON.readTree(messageBody);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Message body is not JSON", e);
        }
        if (root == null || !root.isObject()) {
            throw new IllegalArgumentException("Message body is not a JSON object");
        }
        List<EventRecord> records = new ArrayList<>();
        for (JsonNode record : root.path("Records")) {
            JsonNode s3 = record.path("s3");
            records.add(new EventRecord(
                    text(record, "eventName"),
                    text(s3.path("bucket"), "name"),
                    text(s3.path("object"), "key"),
                    instant(record, "eventTime")));
        }
        return new S3EventNotification(List.copyOf(records), text(root, "Event"));
    }

    public boolean isTestEvent() {
        return "s3:TestEvent".equals(event);
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.path(field);
        return value.isTextual() ? value.asText() : null;
    }

    /** Only feeds a metric, so a missing or malformed timestamp must not fail the message. */
    private static Instant instant(JsonNode node, String field) {
        String value = text(node, field);
        if (value == null) {
            return null;
        }
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    /**
     * @param encodedKey the object key as S3 sends it: URL-encoded, with spaces as {@code +}
     * @param eventTime  when S3 created the event, i.e. when the upload finished; null if absent
     */
    public record EventRecord(String eventName, String bucketName, String encodedKey, Instant eventTime) {

        public boolean isObjectCreated() {
            return eventName != null && eventName.startsWith("ObjectCreated:") && bucketName != null && encodedKey != null;
        }

        public String objectKey() {
            return URLDecoder.decode(encodedKey, StandardCharsets.UTF_8);
        }
    }
}
