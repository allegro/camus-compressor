package pl.allegro.tech.hadoop.compressor.mode;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TopicCompressorTest {
    private static final FileStatus[] EMPTY_STATUSES = {};

    @Mock
    private FileSystem fileSystem;

    @Mock
    private UnitCompressor unitCompressor;

    @Mock
    private CompressorOptions options;

    private TopicCompressor topicCompressor;

    @Before
    public void setUp() {
        when(options.getTopicModePatterns()).thenReturn(Arrays.asList("hourly/*/*/*/*", "daily/*/*/*"));
        topicCompressor = new TopicCompressor(fileSystem, unitCompressor, new TopicDateFilter(1), options);
    }

    @Test
    public void shouldCompressForAllHours() throws Exception {
        // given
        Path hour1 = new Path("test_hour_path_1");
        Path hour2 = new Path("test_hour_path_2");
        when(fileSystem.globStatus(new Path("topic_dir/hourly/*/*/*/*"))).thenReturn(new FileStatus[]{
                fileStatusForPath(hour1), fileStatusForPath(hour2)
        });
        when(fileSystem.globStatus(new Path("topic_dir/daily/*/*/*"))).thenReturn(EMPTY_STATUSES);

        // when
        topicCompressor.compress("topic_dir");

        // then
        verify(unitCompressor).compress(hour1);
        verify(unitCompressor).compress(hour2);
        verifyNoMoreInteractions(unitCompressor);

    }

    @Test
    public void shouldCompressForAllHDays() throws Exception {
        // given
        Path day1 = new Path("test_day_path_1");
        Path day2 = new Path("test_day_path_2");
        when(fileSystem.globStatus(new Path("topic_dir/hourly/*/*/*/*"))).thenReturn(EMPTY_STATUSES);
        when(fileSystem.globStatus(new Path("topic_dir/daily/*/*/*"))).thenReturn(new FileStatus[] {
                fileStatusForPath(day1), fileStatusForPath(day2)
        });

        // when
        topicCompressor.compress("topic_dir");

        // then
        verify(unitCompressor).compress(day1);
        verify(unitCompressor).compress(day2);
        verifyNoMoreInteractions(unitCompressor);

    }

    @Test
    public void shouldNotCompressFilesForToday() throws IOException {
        // given
        Calendar cal = Calendar.getInstance();
        Path todayPath = new Path(String.format("test/%4d/%02d/%02d/today", cal.get(YEAR), cal.get(MONTH)+1, cal.get(DAY_OF_MONTH)));
        when(fileSystem.globStatus(new Path("topic_dir/hourly/*/*/*/*"))).thenReturn(new FileStatus[] {
                fileStatusForPath(todayPath)
        });
        when(fileSystem.globStatus(new Path("topic_dir/daily/*/*/*"))).thenReturn(EMPTY_STATUSES);

        // when
        topicCompressor.compress("topic_dir");

        // then
        verifyZeroInteractions(unitCompressor);
    }

    @Test
    public void shouldNotAvoidCompressingFilesWithStrangePath() throws IOException {
        // given
        Calendar cal = Calendar.getInstance();
        String todayDir = String.format("%4d/%02d/%02d", cal.get(YEAR), cal.get(MONTH)+1, cal.get(DAY_OF_MONTH));
        cal.add(DAY_OF_MONTH, -1);
        String yesterdayDir = String.format("%4d/%02d/%02d", cal.get(YEAR), cal.get(MONTH)+1, cal.get(DAY_OF_MONTH));
        String base = String.format("/camus/%s/mytopic", todayDir);
        when(fileSystem.globStatus(new Path(String.format("%s/hourly/*/*/*/*", base)))).thenReturn(EMPTY_STATUSES);
        when(fileSystem.globStatus(new Path(String.format("%s/daily/*/*/*", base)))).thenReturn(new FileStatus[]{
                fileStatusForPath(String.format("%s/%s", base, todayDir)),
                fileStatusForPath(String.format("%s/%s", base, yesterdayDir))
        });

        // when
        topicCompressor.compress(base);

        // then
        verify(unitCompressor).compress(new Path(String.format("%s/%s", base, yesterdayDir)));
        verifyNoMoreInteractions(unitCompressor);
    }

    private static FileStatus fileStatusForPath(Path path) {
        return new FileStatus(10, true, 3, 1024, 100, path);
    }

    private static FileStatus fileStatusForPath(String path) {
        return fileStatusForPath(new Path(path));
    }
}