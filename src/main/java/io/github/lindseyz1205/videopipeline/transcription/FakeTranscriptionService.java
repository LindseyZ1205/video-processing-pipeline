package io.github.lindseyz1205.videopipeline.transcription;

/**
 * Default provider. It makes no external calls, so the pipeline runs anywhere (LocalStack, CI) without an API key.
 */
public class FakeTranscriptionService implements TranscriptionService {

    @Override
    public String transcribe(StoredObject object) {
        return "[fake transcript] %s (%d bytes, %s)".formatted(object.key(), object.sizeBytes(), object.contentType());
    }
}
