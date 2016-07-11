package pl.allegro.tech.hadoop.compressor.mode;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import pl.allegro.tech.hadoop.compressor.option.CompressorOptions;

import java.io.IOException;
import java.util.Arrays;

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

    @Mock
    private CompressorOptions options;

    private CamusCompressor camusCompressor;

    @Before
    public void setUp() throws Exception {
        when(options.getAllModeExcludes()).thenReturn(Arrays.asList("integration", "history", "base"));
        when(options.getAllModeTimeout()).thenReturn(1440L);
        camusCompressor = new CamusCompressor(fileSystem, topicCompressor, 1, options);
    }

    @Test
    public void shouldCompressTopicsThatMatchPattern() throws IOException {
        // given
        FileStatus[] topics = new FileStatus[camusCompressor.excludes.size() + 2];
        topics[0] = getFileStatus("topic1");
        topics[1] = getFileStatus("topic2");
        for (int i = 0; i<camusCompressor.excludes.size(); i++) {
            topics[i + 2] = getFileStatus(camusCompressor.excludes.get(i));
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
