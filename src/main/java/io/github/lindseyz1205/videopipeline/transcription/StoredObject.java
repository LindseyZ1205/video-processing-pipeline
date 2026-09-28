package io.github.lindseyz1205.videopipeline.transcription;

/**
 * An uploaded file as the worker sees it after {@code HeadObject}.
 */
public record StoredObject(String bucket, String key, long sizeBytes, String contentType) {

    public String fileName() {
        return key.substring(key.lastIndexOf('/') + 1);
    }
}
