package io.github.lindseyz1205.videopipeline.transcription;

import io.github.lindseyz1205.videopipeline.config.PipelineProperties;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Optional;
import java.util.Set;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.util.Assert;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

/**
 * Transcribes with OpenAI's speech-to-text endpoint, {@code POST /audio/transcriptions}.
 * Enable it with {@code pipeline.transcription.provider=openai} and {@code OPENAI_API_KEY}.
 */
public class OpenAiTranscriptionService implements TranscriptionService {

    /** The endpoint rejects files larger than 25 MB. */
    static final long MAX_FILE_SIZE_BYTES = 25L * 1024 * 1024;

    /** Responses that mean "this file will never work", as opposed to auth, rate-limit or server trouble. */
    private static final Set<Integer> FILE_REJECTED = Set.of(400, 413, 415, 422);
    /** Too Many Requests and Service Unavailable: their {@code Retry-After} says when to come back. */
    private static final Set<Integer> COME_BACK_LATER = Set.of(429, 503);

    private final RestClient restClient;
    private final S3Client s3;
    private final String model;
    private final Clock clock;

    public OpenAiTranscriptionService(RestClient.Builder restClientBuilder, S3Client s3,
            PipelineProperties.Transcription.OpenAi settings, Clock clock) {
        Assert.hasText(settings.apiKey(), "pipeline.transcription.openai.api-key (OPENAI_API_KEY) must be set");
        this.restClient = restClientBuilder
                .baseUrl(settings.baseUrl())
                .defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + settings.apiKey())
                .build();
        this.s3 = s3;
        this.model = settings.model();
        this.clock = clock;
    }

    @Override
    public String transcribe(StoredObject object) {
        if (object.sizeBytes() > MAX_FILE_SIZE_BYTES) {
            throw new UnprocessableMediaException(
                    "OpenAI accepts files up to 25 MB; this one is " + object.sizeBytes() + " bytes");
        }
        byte[] media = s3.getObjectAsBytes(GetObjectRequest.builder()
                        .bucket(object.bucket())
                        .key(object.key())
                        .build())
                .asByteArray();

        MultiValueMap<String, Object> form = new LinkedMultiValueMap<>();
        form.add("model", model);
        form.add("file", new ByteArrayResource(media) {
            @Override
            public String getFilename() {
                return object.fileName(); // the API detects the format from the extension
            }
        });

        try {
            TranscriptionResponse response = restClient.post()
                    .uri("/audio/transcriptions")
                    .contentType(MediaType.MULTIPART_FORM_DATA)
                    .body(form)
                    .retrieve()
                    .body(TranscriptionResponse.class);
            return response == null || response.text() == null ? "" : response.text();
        } catch (HttpStatusCodeException e) {
            int status = e.getStatusCode().value();
            if (FILE_REJECTED.contains(status)) {
                throw new UnprocessableMediaException(
                        "OpenAI rejected the file (" + status + "): " + e.getResponseBodyAsString());
            }
            HttpHeaders headers = e.getResponseHeaders();
            Optional<Duration> wait = COME_BACK_LATER.contains(status) && headers != null
                    ? retryAfter(headers.getFirst(HttpHeaders.RETRY_AFTER), clock.instant())
                    : Optional.empty();
            if (wait.isPresent()) {
                throw new RetryLaterException(
                        "OpenAI answered " + status + " and asked to retry after " + wait.get().toSeconds() + " s",
                        wait.get(), e);
            }
            throw e; // 401, 403, other 429s, 5xx: fix the config or wait, then SQS retries
        }
    }

    /**
     * The wait that a {@code Retry-After} value asks for: a number of seconds, or an HTTP date. Empty when there is no
     * value or it can't be read, and the usual backoff applies.
     */
    static Optional<Duration> retryAfter(String value, Instant now) {
        if (value == null || value.isBlank()) {
            return Optional.empty();
        }
        String trimmed = value.trim();
        try {
            if (trimmed.chars().allMatch(Character::isDigit)) {
                return Optional.of(Duration.ofSeconds(Long.parseLong(trimmed)));
            }
            Instant date = ZonedDateTime.parse(trimmed, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant();
            Duration untilThen = Duration.between(now, date);
            return Optional.of(untilThen.isNegative() ? Duration.ZERO : untilThen);
        } catch (NumberFormatException | DateTimeParseException e) {
            return Optional.empty();
        }
    }

    record TranscriptionResponse(String text) {
    }
}
