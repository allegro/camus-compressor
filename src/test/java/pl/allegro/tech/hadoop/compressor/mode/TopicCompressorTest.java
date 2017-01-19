package pl.allegro.tech.hadoop.compressor.mode;

import org.apache.hadoop.fs.BatchedRemoteIterator;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import pl.allegro.tech.hadoop.compressor.mode.unit.UnitCompressor;
import pl.allegro.tech.hadoop.compressor.option.CompressorOptions;
import pl.allegro.tech.hadoop.compressor.util.TopicDateFilter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Calendar;

import static java.util.Calendar.DAY_OF_MONTH;
import static java.util.Calendar.MONTH;
import static java.util.Calendar.YEAR;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TopicCompressorTest {

    private static final RemoteIterator<LocatedFileStatus> EMPTY_STATUSES = new FakeRemoteIterator<>(new LocatedFileStatus[] {});

    @Mock
    private FileSystem fileSystem;

    @Mock
    private UnitCompressor unitCompressor;

    @Mock
    private CompressorOptions options;

    private TopicCompressor topicCompressor;

    @Before
    public void setUp() {
        when(options.getTopicModePatterns()).thenReturn(Arrays.asList("hourly", "daily"));
        topicCompressor = new TopicCompressor(fileSystem, unitCompressor, new TopicDateFilter(1), options);
    }

    @Test
    public void shouldCompressForAllHours() throws Exception {
        // given
        Path hour1 = new Path("/topic_dir/hourly/2016/01/01/10/file1.deflate");
        Path hour2 = new Path("/topic_dir/hourly/2016/01/02/10/file1.deflate");
        when(fileSystem.exists(any(Path.class))).thenReturn(true);
        when(fileSystem.listFiles(new Path("/topic_dir/hourly"), true)).thenReturn(new FakeRemoteIterator<>(new LocatedFileStatus[]{
                fileStatusForPath(hour1), fileStatusForPath(hour2)
        }));
        when(fileSystem.listFiles(new Path("/topic_dir/daily"), true)).thenReturn(EMPTY_STATUSES);

        // when
        topicCompressor.compress("/topic_dir");

        // then
        verify(unitCompressor).compress(hour1.getParent());
        verify(unitCompressor).compress(hour2.getParent());
        verifyNoMoreInteractions(unitCompressor);

    }

    @Test
    public void shouldCompressForAllHDays() throws Exception {
        // given
        Path day1 = new Path("/topic_dir/daily/2016/01/01/file1.deflate");
        Path day2 = new Path("/topic_dir/daily/2016/01/02/file1.deflate");
        when(fileSystem.listFiles(new Path("/topic_dir/hourly"), true)).thenReturn(EMPTY_STATUSES);
        when(fileSystem.exists(any(Path.class))).thenReturn(true);
        when(fileSystem.listFiles(new Path("/topic_dir/daily"), true)).thenReturn(new FakeRemoteIterator<>(new LocatedFileStatus[] {
                fileStatusForPath(day1), fileStatusForPath(day2)
        }));

        // when
        topicCompressor.compress("/topic_dir");

        // then
        verify(unitCompressor).compress(day1.getParent());
        verify(unitCompressor).compress(day2.getParent());
        verifyNoMoreInteractions(unitCompressor);

    }

    @Test
    public void shouldNotCompressFilesForToday() throws IOException {
        // given
        Calendar cal = Calendar.getInstance();
        Path todayPath = new Path(String.format("test/%4d/%02d/%02d/today", cal.get(YEAR), cal.get(MONTH)+1, cal.get(DAY_OF_MONTH)));
        when(fileSystem.listFiles(new Path("topic_dir/hourly"), true)).thenReturn(new FakeRemoteIterator<>(new LocatedFileStatus[] {
                fileStatusForPath(todayPath)
        }));
        when(fileSystem.exists(any(Path.class))).thenReturn(true);
        when(fileSystem.listFiles(new Path("topic_dir/daily"), true)).thenReturn(EMPTY_STATUSES);

        // when
        topicCompressor.compress("topic_dir");

        // then
        verifyZeroInteractions(unitCompressor);
    }

    private static LocatedFileStatus fileStatusForPath(Path path) throws IOException {
        final FileStatus stat = new FileStatus(10, true, 3, 1024, 100, path);
        return new LocatedFileStatus(stat, new BlockLocation[] {});
    }

    private static LocatedFileStatus fileStatusForPath(String path) throws IOException {
        return fileStatusForPath(new Path(path));
    }

    private static class FakeRemoteIterator<E> implements RemoteIterator<E> {
        private E[] elements;
        private int count;

        FakeRemoteIterator(E[] elements) {
            this.elements = elements;
            this.count = 0;
        }

        public boolean hasNext() throws IOException {
            return this.count < this.elements.length;
        }

        public E next() throws IOException {
            return this.hasNext() ? this.elements[this.count++] : null;
        }
    }
}
