package io.github.lindseyz1205.videopipeline.upload;

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
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/uploads")
public class UploadController {

    private final UploadService uploads;

    public UploadController(UploadService uploads) {
        this.uploads = uploads;
    }

    @PostMapping
    public ResponseEntity<CreateUploadResponse> create(@Valid @RequestBody CreateUploadRequest request) {
        CreateUploadResponse upload = uploads.createUpload(request);
        return ResponseEntity.created(URI.create("/api/uploads/" + upload.uploadId())).body(upload);
    }

    @GetMapping("/{uploadId}")
    public ResponseEntity<UploadStatusResponse> status(@PathVariable("uploadId") String uploadId) {
        return ResponseEntity.of(uploads.status(uploadId));
    }

    @ExceptionHandler(UploadRejectedException.class)
    public ProblemDetail rejected(UploadRejectedException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
    }
}
