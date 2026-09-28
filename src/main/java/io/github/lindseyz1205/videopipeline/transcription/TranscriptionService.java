package io.github.lindseyz1205.videopipeline.transcription;

/**
 * Turns an uploaded audio or video file into text.
 *
 * <p>The pipeline delivers at least once, so after a crash the same file can be transcribed again. Implementations
 * must be safe to call more than once for one file.
 */
public interface TranscriptionService {

    /**
     * @throws UnprocessableMediaException if this file can never be transcribed, so retrying would not help
     */
    String transcribe(StoredObject object);
}
