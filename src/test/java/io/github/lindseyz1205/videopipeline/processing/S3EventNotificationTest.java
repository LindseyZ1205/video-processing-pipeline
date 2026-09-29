package io.github.lindseyz1205.videopipeline.processing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class S3EventNotificationTest {

    @Test
    void readsAnObjectCreatedEvent() {
        String body = """
                {"Records":[{"eventVersion":"2.1","eventSource":"aws:s3","awsRegion":"us-east-1",
                  "eventTime":"2026-09-28T19:00:03.512Z","eventName":"ObjectCreated:Put",
                  "s3":{"s3SchemaVersion":"1.0",
                        "bucket":{"name":"video-uploads","arn":"arn:aws:s3:::video-uploads"},
                        "object":{"key":"uploads/user-1/my+talk%281%29.mp4","size":1024,"eTag":"abc"}}}]}
                """;

        S3EventNotification notification = S3EventNotification.parse(body);

        assertThat(notification.isTestEvent()).isFalse();
        assertThat(notification.records()).hasSize(1);
        S3EventNotification.EventRecord record = notification.records().get(0);
        assertThat(record.isObjectCreated()).isTrue();
        assertThat(record.bucketName()).isEqualTo("video-uploads");
        assertThat(record.objectKey()).isEqualTo("uploads/user-1/my talk(1).mp4");
        assertThat(record.eventTime()).isEqualTo(Instant.parse("2026-09-28T19:00:03.512Z"));
    }

    @Test
    void recognisesTheTestEventS3SendsWhenNotificationsAreConfigured() {
        String body = """
                {"Service":"Amazon S3","Event":"s3:TestEvent","Time":"2026-01-01T00:00:00.000Z",
                 "Bucket":"video-uploads","RequestId":"R1","HostId":"H1"}
                """;

        S3EventNotification notification = S3EventNotification.parse(body);

        assertThat(notification.isTestEvent()).isTrue();
        assertThat(notification.records()).isEmpty();
    }

    @Test
    void toleratesIncompleteRecords() {
        S3EventNotification notification = S3EventNotification.parse("""
                {"Records":[{"eventName":"ObjectCreated:Put","eventTime":"yesterday","s3":{"bucket":{"name":"video-uploads"}}}]}
                """);

        S3EventNotification.EventRecord record = notification.records().get(0);
        assertThat(record.isObjectCreated()).isFalse();
        assertThat(record.eventTime()).isNull();
    }

    @Test
    void rejectsBodiesThatAreNotJsonObjects() {
        assertThatThrownBy(() -> S3EventNotification.parse("not json")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> S3EventNotification.parse("[]")).isInstanceOf(IllegalArgumentException.class);
    }
}
