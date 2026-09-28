package io.github.lindseyz1205.videopipeline.upload;

/**
 * The upload request is valid JSON but breaks a business rule (file type, size limit). Returned as 400.
 */
public class UploadRejectedException extends RuntimeException {

    public UploadRejectedException(String message) {
        super(message);
    }
}
