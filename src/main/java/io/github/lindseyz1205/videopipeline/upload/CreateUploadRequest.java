package io.github.lindseyz1205.videopipeline.upload;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

/**
 * @param userId who owns the upload. In a real deployment this comes from the authenticated caller, not the body.
 */
public record CreateUploadRequest(
        @NotBlank @Pattern(regexp = "[A-Za-z0-9_-]{1,64}", message = "must be 1-64 letters, digits, '_' or '-'")
        String userId,
        @NotBlank @Size(max = 255) String fileName,
        @NotBlank String contentType,
        @Positive long sizeBytes) {
}
