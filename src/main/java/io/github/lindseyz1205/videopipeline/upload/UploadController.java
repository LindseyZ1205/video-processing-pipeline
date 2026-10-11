package io.github.lindseyz1205.videopipeline.upload;

import io.github.lindseyz1205.videopipeline.job.IdempotencyKeyInUseException;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import java.net.URI;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/uploads")
@Tag(name = "Uploads", description = "Presigned uploads and the transcription jobs they start")
public class UploadController {

    private final UploadService uploads;

    public UploadController(UploadService uploads) {
        this.uploads = uploads;
    }

    @Operation(
            summary = "Request an upload",
            description = "Checks the file type and declared size, records a PENDING_UPLOAD job, and returns a "
                    + "presigned URL. PUT the file to that URL with the returned headers; the job starts once S3 has it.")
    @ApiResponse(responseCode = "201", description = "Upload created, or replayed for a retried Idempotency-Key "
            + "(then with the header Idempotent-Replayed: true)")
    @ApiResponse(responseCode = "400", description = "Not an audio or video file, over the size limit, or invalid fields",
            content = @Content(schema = @Schema(implementation = ProblemDetail.class)))
    @ApiResponse(responseCode = "409", description = "A request with the same Idempotency-Key is still being processed",
            content = @Content(schema = @Schema(implementation = ProblemDetail.class)))
    @ApiResponse(responseCode = "422", description = "The Idempotency-Key was already used for a different upload",
            content = @Content(schema = @Schema(implementation = ProblemDetail.class)))
    @PostMapping
    public ResponseEntity<CreateUploadResponse> create(@Valid @RequestBody CreateUploadRequest request,
            @Parameter(description = "Optional, for safe retries: a request with the same key and the same details "
                    + "returns the upload the first one created. Kept for 24 hours, per user.")
            @RequestHeader(name = "Idempotency-Key", required = false) String idempotencyKey) {
        UploadService.Created created = uploads.createUpload(request, idempotencyKey);
        ResponseEntity.BodyBuilder response =
                ResponseEntity.created(URI.create("/api/uploads/" + created.upload().uploadId()));
        if (created.replayed()) {
            response.header("Idempotent-Replayed", "true");
        }
        return response.body(created.upload());
    }

    @Operation(summary = "Get an upload's job", description = "Status, attempts so far, and the transcript once completed.")
    @ApiResponse(responseCode = "200", description = "The job")
    @ApiResponse(responseCode = "404", description = "No upload with this ID", content = @Content)
    @GetMapping("/{uploadId}")
    public ResponseEntity<UploadStatusResponse> status(@PathVariable("uploadId") String uploadId) {
        return ResponseEntity.of(uploads.status(uploadId));
    }

    @ExceptionHandler(UploadRejectedException.class)
    public ProblemDetail rejected(UploadRejectedException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
    }

    @ExceptionHandler(IdempotencyKeyReusedException.class)
    public ProblemDetail keyReused(IdempotencyKeyReusedException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.UNPROCESSABLE_ENTITY, e.getMessage());
    }

    @ExceptionHandler(IdempotencyKeyInUseException.class)
    public ProblemDetail keyInUse(IdempotencyKeyInUseException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, e.getMessage());
    }
}
