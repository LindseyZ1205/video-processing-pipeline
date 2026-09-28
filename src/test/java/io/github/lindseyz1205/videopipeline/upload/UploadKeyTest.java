package io.github.lindseyz1205.videopipeline.upload;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class UploadKeyTest {

    @Test
    void newUploadHasAParsableKey() {
        UploadKey key = UploadKey.newUpload("user-1", "Team sync (final).mp4");

        assertThat(key.objectKey()).startsWith("uploads/user-1/").endsWith("/Team_sync__final_.mp4");
        assertThat(UploadKey.parse(key.objectKey())).contains(key);
        assertThat(UploadKey.isUploadId(key.uploadId())).isTrue();
    }

    @Test
    void fileNamesLoseDirectoriesAndUnsafeCharacters() {
        assertThat(UploadKey.sanitize("C:\\Users\\me\\talk.mp4")).isEqualTo("talk.mp4");
        assertThat(UploadKey.sanitize("../../etc/passwd")).isEqualTo("passwd");
        assertThat(UploadKey.sanitize("..")).isEqualTo("file");
        assertThat(UploadKey.sanitize("vidéo 1.mp4")).isEqualTo("vid_o_1.mp4");
    }

    @Test
    void keysOutsideTheUploadLayoutAreNotUploads() {
        assertThat(UploadKey.parse("transcripts/summary.txt")).isEmpty();
        assertThat(UploadKey.parse("uploads/user-1/not-a-uuid/talk.mp4")).isEmpty();
        assertThat(UploadKey.parse("uploads/user-1/0f8fad5b-d9cb-469f-a165-70867728950e/nested/talk.mp4")).isEmpty();
    }
}
