package io.github.lindseyz1205.videopipeline.upload;

import java.util.Optional;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Object key layout for uploads: {@code uploads/{userId}/{uploadId}/{fileName}}.
 *
 * <p>Every presigned URL gets a fresh upload ID, so the ID names exactly one job. The worker reads it back from the
 * key in the S3 event and uses it as the idempotency key.
 */
public record UploadKey(String userId, String uploadId, String fileName) {

    public static final String PREFIX = "uploads/";

    private static final String UUID_PATTERN = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";
    private static final Pattern UPLOAD_ID = Pattern.compile(UUID_PATTERN);
    private static final Pattern LAYOUT = Pattern.compile("uploads/([A-Za-z0-9_-]{1,64})/(" + UUID_PATTERN + ")/([^/]+)");
    private static final int MAX_FILE_NAME_LENGTH = 200;

    public static UploadKey newUpload(String userId, String fileName) {
        return new UploadKey(userId, UUID.randomUUID().toString(), sanitize(fileName));
    }

    public static Optional<UploadKey> parse(String objectKey) {
        Matcher matcher = LAYOUT.matcher(objectKey);
        if (!matcher.matches()) {
            return Optional.empty();
        }
        return Optional.of(new UploadKey(matcher.group(1), matcher.group(2), matcher.group(3)));
    }

    public static boolean isUploadId(String value) {
        return UPLOAD_ID.matcher(value).matches();
    }

    public String objectKey() {
        return PREFIX + userId + "/" + uploadId + "/" + fileName;
    }

    /** Keeps the name recognisable in the S3 console but safe as a key segment: no directories, no odd characters. */
    static String sanitize(String fileName) {
        String name = fileName.replace('\\', '/');
        name = name.substring(name.lastIndexOf('/') + 1).replaceAll("[^A-Za-z0-9._-]", "_");
        if (name.length() > MAX_FILE_NAME_LENGTH) {
            name = name.substring(name.length() - MAX_FILE_NAME_LENGTH); // keep the extension
        }
        return name.isEmpty() || name.chars().allMatch(c -> c == '.') ? "file" : name;
    }
}
