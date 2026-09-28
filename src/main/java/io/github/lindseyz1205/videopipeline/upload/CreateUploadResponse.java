package io.github.lindseyz1205.videopipeline.upload;

import java.time.Instant;
import java.util.Map;

/**
 * @param headers headers the client must send with the upload. They are part of the signature, so S3 rejects the
 *                request if they are missing or different.
 */
public record CreateUploadResponse(
        String uploadId,
        String objectKey,
        String uploadUrl,
        String method,
        Map<String, String> headers,
        Instant expiresAt) {
}
