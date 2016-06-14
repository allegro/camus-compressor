package pl.allegro.tech.hadoop.compressor.mode;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CamusCompressorTest {
    private static final String PATH_PREFIX = "/some/dir/";

    @Mock
    private FileSystem fileSystem;

    @Mock
    private TopicCompressor topicCompressor;

    private CamusCompressor camusCompressor;

    @Before
    public void setUp() throws Exception {
        camusCompressor = new CamusCompressor(fileSystem, topicCompressor, 1);
    }

    @Test
    public void shouldCompressTopicsThatMatchPattern() throws IOException {
        // given
        FileStatus[] topics = new FileStatus[CamusCompressor.EXCLUDES.size() + 2];
        topics[0] = getFileStatus("topic1");
        topics[1] = getFileStatus("topic2");
        for (int i = 0; i<CamusCompressor.EXCLUDES.size(); i++) {
            topics[i + 2] = getFileStatus(CamusCompressor.EXCLUDES.get(i));
        }

        when(fileSystem.listStatus(new Path("camus_dir"))).thenReturn(topics);

        // when
        camusCompressor.compress("camus_dir");

        // then
        verify(topicCompressor).compress(new Path(PATH_PREFIX + "topic1"));
        verify(topicCompressor).compress(new Path(PATH_PREFIX + "topic2"));
        verifyNoMoreInteractions(topicCompressor);
    }

    private FileStatus getFileStatus(String path) {
        FileStatus fileStatus = new FileStatus();
        fileStatus.setPath(new Path(PATH_PREFIX + path));
        return fileStatus;
    }

}
