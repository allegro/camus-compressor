package pl.allegro.tech.hadoop.compressor.util;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import pl.allegro.tech.hadoop.compressor.compression.Compression;
import pl.allegro.tech.hadoop.compressor.option.FilesFormat;

import java.io.IOException;
import java.util.Calendar;

import static java.util.Calendar.DAY_OF_MONTH;
import static java.util.Calendar.MONTH;
import static java.util.Calendar.YEAR;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class InputAnalyserTest {

    @Mock
    private FileSystem fileSystem;
    @Mock
    private JavaSparkContext sparkContext;

    @Mock
    private Compression<LongWritable, NullWritable, Text> snappyCompression;

    private static final FileStatus[] EMPTY_STATUSES = {};
    private static final String RAW_FILE = "part-0001";
    private static final String COMPRESSED_FILE = "part-0001.snappy";

    @Before
    public void setUp() throws IOException {
        String todayDir = getTodayDir();
        String yesterdayDir = getYesterdayDir();

        String topicLocation = getTopicLocation();
        when(snappyCompression.getSplits(anyInt())).thenReturn(1);
        when(snappyCompression.getExtension()).thenReturn("snappy");
        when(fileSystem.globStatus(new Path(String.format("%s/hourly/*/*/*/*", topicLocation)))).thenReturn(EMPTY_STATUSES);
        when(fileSystem.globStatus(new Path(String.format("%s/daily/%s", topicLocation, yesterdayDir)))).thenReturn(new FileStatus[]{
                createFileStatusForPath(String.format("%s/%s/%s", topicLocation, todayDir, COMPRESSED_FILE)),
                createFileStatusForPath(String.format("%s/%s/%s", topicLocation, yesterdayDir, COMPRESSED_FILE))
        });
    }

    @Test
    public void shouldCompressWhenForced() throws IOException {
        //given
        boolean forceSplit = true;
        String yesterdayDir = getYesterdayDir();
        String topicLocation = getTopicLocation();

        InputAnalyser analyser = new InputAnalyser(fileSystem, FilesFormat.JSON, snappyCompression, forceSplit);

        //when
        boolean shouldCompress = analyser.shouldCompress(String.format("%s/daily/%s", topicLocation, yesterdayDir));

        //then
        assertTrue(shouldCompress);
    }

    @Test
    public void shouldNotCompressWhenNotForcedAndAllFilesCompressed() throws IOException {
        //given
        boolean forceSplit = false;
        String yesterdayDir = getYesterdayDir();
        String topicLocation = getTopicLocation();
        InputAnalyser analyser = new InputAnalyser(fileSystem, FilesFormat.JSON, snappyCompression, forceSplit);

        //when
        boolean shouldCompress = analyser.shouldCompress(String.format("%s/daily/%s", topicLocation, yesterdayDir));

        //then
        assertFalse(shouldCompress);
    }

    @Test
    public void shouldCompressWhenRawFilesFound() throws IOException {
        //given
        String todayDir = getTodayDir();
        String yesterdayDir = getYesterdayDir();
        String topicLocation = getTopicLocation();
        when(fileSystem.globStatus(new Path(String.format("%s/hourly/*/*/*/*", topicLocation)))).thenReturn(EMPTY_STATUSES);
        when(fileSystem.globStatus(new Path(String.format("%s/daily/%s", topicLocation, yesterdayDir)))).thenReturn(new FileStatus[]{
                createFileStatusForPath(String.format("%s/%s/%s", topicLocation, todayDir, RAW_FILE)),
                createFileStatusForPath(String.format("%s/%s/%s", topicLocation, yesterdayDir, RAW_FILE))
        });
        boolean forceSplit = false;
        InputAnalyser analyser = new InputAnalyser(fileSystem, FilesFormat.JSON, snappyCompression, forceSplit);

        //when
        boolean shouldCompress = analyser.shouldCompress(String.format("%s/daily/%s", topicLocation, yesterdayDir));

        //then
        assertTrue(shouldCompress);
    }

    @Test
    public void shouldCountInputSize() throws IOException {
        //given
        boolean forceSplit = false;
        String yesterdayDir = getYesterdayDir();
        String topicLocation = getTopicLocation();
        InputAnalyser analyser = new InputAnalyser(fileSystem, FilesFormat.JSON, snappyCompression, forceSplit);

        //when
        long inputSize = analyser.countInputSize(String.format("%s/daily/%s", topicLocation, yesterdayDir));

        //then
        assertTrue(inputSize == 2048L);
    }

    @Test
    public void shouldCountInputSplits() throws IOException {
        //given
        boolean forceSplit = false;
        String yesterdayDir = getYesterdayDir();
        String topicLocation = getTopicLocation();
        InputAnalyser analyser = new InputAnalyser(fileSystem, FilesFormat.JSON, snappyCompression, forceSplit);

        //when
        int inputSplits = analyser.countInputSplits(String.format("%s/daily/%s", topicLocation, yesterdayDir));

        //then
        assertTrue(inputSplits == 1);
    }

    private String getTopicLocation() {
        return "/topics/mytopic";
    }

    private String getTodayDir() {
        Calendar cal = Calendar.getInstance();
        return String.format("%4d/%02d/%02d", cal.get(YEAR), cal.get(MONTH) + 1, cal.get(DAY_OF_MONTH));
    }

    private String getYesterdayDir() {
        Calendar cal = Calendar.getInstance();
        cal.add(DAY_OF_MONTH, -1);
        return String.format("%4d/%02d/%02d", cal.get(YEAR), cal.get(MONTH) + 1, cal.get(DAY_OF_MONTH));
    }

    private FileStatus createFileStatusForPath(String fileLocation) {
        return new FileStatus(1024L, false, 3, 1024L, 100, new Path(fileLocation));
    }
}
