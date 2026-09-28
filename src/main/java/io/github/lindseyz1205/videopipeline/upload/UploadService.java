package io.github.lindseyz1205.videopipeline.upload;

import io.github.lindseyz1205.videopipeline.config.PipelineProperties;
import io.github.lindseyz1205.videopipeline.job.JobStore;
import java.time.Duration;
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
     * Registers the upload and returns a short-lived URL the client uses to PUT the file straight into S3, so the
     * file's bytes never pass through this service.
     */
    public CreateUploadResponse createUpload(CreateUploadRequest request) {
        if (!MEDIA_TYPE.matcher(request.contentType()).matches()) {
            throw new UploadRejectedException("Only audio and video files can be transcribed, got " + request.contentType());
        }
        // A presigned PUT cannot cap the size by itself, so this checks the declared size and the worker checks
        // the real one before processing.
        if (request.sizeBytes() > maxFileSize.toBytes()) {
            throw new UploadRejectedException("Files are limited to " + maxFileSize.toMegabytes() + " MB");
        }

        UploadKey key = UploadKey.newUpload(request.userId(), request.fileName());
        jobs.createPending(key.uploadId(), bucket, key.objectKey());

        PresignedPutObjectRequest presigned = presigner.presignPutObject(PutObjectPresignRequest.builder()
                .signatureDuration(urlTtl)
                // Content-Type is signed: S3 rejects the upload if the client sends a different one.
                .putObjectRequest(PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(key.objectKey())
                        .contentType(request.contentType())
                        .build())
                .build());

        return new CreateUploadResponse(
                key.uploadId(),
                key.objectKey(),
                presigned.url().toString(),
                presigned.httpRequest().method().name(),
                Map.of("Content-Type", request.contentType()),
                presigned.expiration());
    }

    public Optional<UploadStatusResponse> status(String uploadId) {
        if (!UploadKey.isUploadId(uploadId)) {
            return Optional.empty();
        }
        return jobs.find(uploadId).map(UploadStatusResponse::from);
    }
}
