package io.github.lindseyz1205.videopipeline.transcription;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.header;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withBadRequest;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import com.sun.net.httpserver.HttpServer;
import io.github.lindseyz1205.videopipeline.config.PipelineProperties;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

class OpenAiTranscriptionServiceTest {

    private static final String ENDPOINT = "https://api.openai.test/v1/audio/transcriptions";

    private final RestClient.Builder restClient = RestClient.builder();
    private final MockRestServiceServer openAi = MockRestServiceServer.bindTo(restClient).build();
    private final S3Client s3 = mock(S3Client.class);
    private final OpenAiTranscriptionService service = new OpenAiTranscriptionService(restClient, s3,
            new PipelineProperties.Transcription.OpenAi("sk-test", "whisper-1", "https://api.openai.test/v1",
                    Duration.ofMinutes(5)));

    private final StoredObject talk = new StoredObject("video-uploads", "uploads/user-1/id/talk.mp3", 3, "audio/mpeg");

    @BeforeEach
    void fileIsInS3() {
        when(s3.getObjectAsBytes(any(GetObjectRequest.class)))
                .thenReturn(ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), new byte[] {1, 2, 3}));
    }

    @Test
    void sendsTheFileAsMultipartAndReturnsTheText() {
        openAi.expect(requestTo(ENDPOINT))
                .andExpect(method(HttpMethod.POST))
                .andExpect(header(HttpHeaders.AUTHORIZATION, "Bearer sk-test"))
                .andExpect(header(HttpHeaders.CONTENT_TYPE, startsWith(MediaType.MULTIPART_FORM_DATA_VALUE)))
                .andRespond(withSuccess("{\"text\":\"hello world\"}", MediaType.APPLICATION_JSON));

        assertThat(service.transcribe(talk)).isEqualTo("hello world");
        openAi.verify();
    }

    @Test
    void aRejectedFileIsAPermanentFailure() {
        openAi.expect(requestTo(ENDPOINT))
                .andRespond(withBadRequest()
                        .contentType(MediaType.APPLICATION_JSON)
                        .body("{\"error\":{\"message\":\"Invalid file format.\"}}"));

        assertThatThrownBy(() -> service.transcribe(talk))
                .isInstanceOf(UnprocessableMediaException.class)
                .hasMessageContaining("Invalid file format");
    }

    @Test
    void rateLimitsAreLeftToTheRetryPolicy() {
        openAi.expect(requestTo(ENDPOINT)).andRespond(withStatus(HttpStatus.TOO_MANY_REQUESTS));

        assertThatThrownBy(() -> service.transcribe(talk)).isInstanceOf(HttpClientErrorException.class);
    }

    @Test
    void filesOverTheApiLimitAreRejectedWithoutDownloadingThem() {
        StoredObject longRecording =
                new StoredObject("video-uploads", "uploads/user-1/id/all-hands.mp4", 30L * 1024 * 1024, "video/mp4");

        assertThatThrownBy(() -> service.transcribe(longRecording)).isInstanceOf(UnprocessableMediaException.class);
        verifyNoInteractions(s3);
    }

    @Test
    @Timeout(10) // without a timeout of its own, the client would wait for good
    void aRequestThatNeverGetsAnAnswerTimesOut() throws IOException, InterruptedException {
        // Takes the request and holds it without answering, like a provider that hangs.
        CountDownLatch requestReceived = new CountDownLatch(1);
        CountDownLatch testOver = new CountDownLatch(1);
        HttpServer silentProvider = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        silentProvider.createContext("/", exchange -> {
            requestReceived.countDown();
            try {
                testOver.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        silentProvider.start();
        try {
            String baseUrl = "http://127.0.0.1:" + silentProvider.getAddress().getPort() + "/v1";
            var settings = new PipelineProperties.Transcription.OpenAi("sk-test", "whisper-1", baseUrl,
                    Duration.ofMillis(300));
            var impatient = TranscriptionConfiguration.openAi(RestClient.builder(), s3, settings);

            // The provider is still holding the request, so only the client's timeout can end the call.
            assertThatThrownBy(() -> impatient.transcribe(talk)).isInstanceOf(ResourceAccessException.class);
            assertThat(requestReceived.await(1, TimeUnit.SECONDS)).isTrue();
        } finally {
            testOver.countDown();
            silentProvider.stop(0);
        }
    }
}
