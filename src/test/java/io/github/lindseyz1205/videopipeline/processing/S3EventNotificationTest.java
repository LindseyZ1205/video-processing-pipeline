package io.github.lindseyz1205.videopipeline.processing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class S3EventNotificationTest {

    @Test
    void readsBucketAndDecodedKeyFromAnObjectCreatedEvent() {
        String body = """
                {"Records":[{"eventVersion":"2.1","eventSource":"aws:s3","awsRegion":"us-east-1",
                  "eventName":"ObjectCreated:Put",
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
    void recordsWithoutAnObjectAreNotObjectCreatedEvents() {
        S3EventNotification notification = S3EventNotification.parse("""
                {"Records":[{"eventName":"ObjectCreated:Put","s3":{"bucket":{"name":"video-uploads"}}}]}
                """);

        assertThat(notification.records().get(0).isObjectCreated()).isFalse();
    }

    @Test
    void rejectsBodiesThatAreNotJsonObjects() {
        assertThatThrownBy(() -> S3EventNotification.parse("not json")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> S3EventNotification.parse("[]")).isInstanceOf(IllegalArgumentException.class);
    }
}
