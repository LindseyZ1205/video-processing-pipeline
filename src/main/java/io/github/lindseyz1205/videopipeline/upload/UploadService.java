package io.github.lindseyz1205.videopipeline.upload;

import io.github.lindseyz1205.videopipeline.config.PipelineProperties;
import io.github.lindseyz1205.videopipeline.job.IdempotencyRecord;
import io.github.lindseyz1205.videopipeline.job.JobStore;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import org.springframework.stereotype.Service;
import org.springframework.util.unit.DataSize;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

@Service
public class UploadService {

    private static final Pattern MEDIA_TYPE = Pattern.compile("(audio|video)/[A-Za-z0-9.+-]+");
    private static final Pattern IDEMPOTENCY_KEY = Pattern.compile("[A-Za-z0-9_.:-]{1,128}");

    private final S3Presigner presigner;
    private final JobStore jobs;
    private final String bucket;
    private final Duration urlTtl;
    private final DataSize maxFileSize;

    public UploadService(S3Presigner presigner, JobStore jobs, PipelineProperties properties) {
        this.presigner = presigner;
        this.jobs = jobs;
        this.bucket = properties.bucket();
        this.urlTtl = properties.upload().urlTtl();
        this.maxFileSize = properties.upload().maxFileSize();
    }

    /**
     * An upload, and whether it is a replay: a retried request with an idempotency key that already created it.
     */
    public record Created(CreateUploadResponse upload, boolean replayed) {
    }

    /**
     * Registers the upload and returns a short-lived URL the client uses to PUT the file straight into S3, so the
     * file's bytes never pass through this service.
     *
     * @param idempotencyKey optional. A retried request with the same key and the same details gets the same upload
     *                       back, with a freshly signed URL, instead of a second one.
     */
    public Created createUpload(CreateUploadRequest request, String idempotencyKey) {
        if (!MEDIA_TYPE.matcher(request.contentType()).matches()) {
            throw new UploadRejectedException("Only audio and video files can be transcribed, got " + request.contentType());
        }
        // A presigned PUT cannot cap the size by itself, so this checks the declared size and the worker checks
        // the real one before processing.
        if (request.sizeBytes() > maxFileSize.toBytes()) {
            throw new UploadRejectedException("Files are limited to " + maxFileSize.toMegabytes() + " MB");
        }

        if (idempotencyKey != null && !IDEMPOTENCY_KEY.matcher(idempotencyKey).matches()) {
            throw new UploadRejectedException("Idempotency-Key must be 1-128 letters, digits, '-', '_', '.' or ':'");
        }

        UploadKey key = UploadKey.newUpload(request.userId(), request.fileName());
        boolean replayed = false;
        if (idempotencyKey == null) {
            jobs.createPending(key.uploadId(), bucket, key.objectKey());
        } else {
            String requestHash = hash(request);
            // Scoped to the user, so two users who pick the same key don't see each other's uploads.
            Optional<IdempotencyRecord> earlier = jobs.createPendingOnce(key.uploadId(), bucket, key.objectKey(),
                    request.userId() + "#" + idempotencyKey, requestHash);
            if (earlier.isPresent()) {
                if (!earlier.get().requestHash().equals(requestHash)) {
                    throw new IdempotencyKeyReusedException();
                }
                key = UploadKey.parse(earlier.get().objectKey()).orElseThrow();
                replayed = true;
            }
        }

        PresignedPutObjectRequest presigned = presigner.presignPutObject(PutObjectPresignRequest.builder()
                .signatureDuration(urlTtl)
                // Content-Type is signed: S3 rejects the upload if the client sends a different one.
                .putObjectRequest(PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(key.objectKey())
                        .contentType(request.contentType())
                        .build())
                .build());

        return new Created(new CreateUploadResponse(
                key.uploadId(),
                key.objectKey(),
                presigned.url().toString(),
                presigned.httpRequest().method().name(),
                Map.of("Content-Type", request.contentType()),
                presigned.expiration()), replayed);
    }

    /** What makes two requests with the same key the same request. The user is part of the key itself. */
    private static String hash(CreateUploadRequest request) {
        // Length-prefixed, so no file name can make two different requests look alike.
        String fields = request.fileName().length() + ":" + request.fileName()
                + request.contentType().length() + ":" + request.contentType()
                + request.sizeBytes();
        try {
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(sha256.digest(fields.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Every JVM has SHA-256", e);
        }
    }

    public Optional<UploadStatusResponse> status(String uploadId) {
        if (!UploadKey.isUploadId(uploadId)) {
            return Optional.empty();
        }
        return jobs.find(uploadId).map(UploadStatusResponse::from);
    }
}
