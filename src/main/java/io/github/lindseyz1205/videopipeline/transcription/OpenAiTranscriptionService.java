package io.github.lindseyz1205.videopipeline.transcription;

import io.github.lindseyz1205.videopipeline.config.PipelineProperties;
import java.util.Set;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.util.Assert;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.HttpClientErrorException;
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

    private final RestClient restClient;
    private final S3Client s3;
    private final String model;

    public OpenAiTranscriptionService(RestClient.Builder restClientBuilder, S3Client s3,
            PipelineProperties.Transcription.OpenAi settings) {
        Assert.hasText(settings.apiKey(), "pipeline.transcription.openai.api-key (OPENAI_API_KEY) must be set");
        this.restClient = restClientBuilder
                .baseUrl(settings.baseUrl())
                .defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + settings.apiKey())
                .build();
        this.s3 = s3;
        this.model = settings.model();
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
        } catch (HttpClientErrorException e) {
            if (FILE_REJECTED.contains(e.getStatusCode().value())) {
                throw new UnprocessableMediaException(
                        "OpenAI rejected the file (" + e.getStatusCode().value() + "): " + e.getResponseBodyAsString());
            }
            throw e; // 401, 403, 429...: fix the config or wait, then SQS retries
        }
    }

    record TranscriptionResponse(String text) {
    }
}
