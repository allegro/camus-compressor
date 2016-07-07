package pl.allegro.tech.hadoop.compressor.mode.unit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import pl.allegro.tech.hadoop.compressor.compression.Compression;
import pl.allegro.tech.hadoop.compressor.exception.InvalidCountsException;
import pl.allegro.tech.hadoop.compressor.option.FilesFormat;
import pl.allegro.tech.hadoop.compressor.util.InputAnalyser;

import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.AdditionalMatchers.not;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static pl.allegro.tech.hadoop.compressor.Utils.fileStatusForEmptyFile;
import static pl.allegro.tech.hadoop.compressor.Utils.fileStatusForPath;

@RunWith(MockitoJUnitRunner.class)
public class JsonUnitCompressorTest {

    @Mock
    private JavaSparkContext sparkContext;

    @Mock
    private FileSystem fileSystem;

    @Mock
    private Compression<LongWritable, NullWritable, Text> compression;

    @Mock
    private JavaPairRDD<LongWritable, Text> testRDD;

    @Mock
    private JavaPairRDD<NullWritable, Text> saveTestRDD;

    private UnitCompressor unitCompressor;

    @Before
    public void setUp() throws Exception {
        InputAnalyser analyser = new InputAnalyser(fileSystem, FilesFormat.JSON, compression, false);
        unitCompressor = new JsonUnitCompressor(sparkContext, fileSystem, WORKING_PATH, compression, analyser, true);
        when(compression.openUncompressed(anyString())).thenReturn(testRDD);
        when(testRDD.count()).thenReturn(10L);
        when(testRDD.mapToPair(any(PairFunction.class))).thenReturn(saveTestRDD);
        when(sparkContext.hadoopConfiguration()).thenReturn(new Configuration());
    }

    @Test
    public void shouldCompress() throws Exception {
        // given
        when(sparkContext.hadoopFile(eq(UNIT_PATH_NAME), eq(TextInputFormat.class),
                eq(LongWritable.class), eq(Text.class))).thenReturn(testRDD);
        when(testRDD.repartition(anyInt())).thenReturn(testRDD);
        when(fileSystem.globStatus(any(Path.class))).thenReturn(TEST_STATUSES);
        when(compression.getExtension()).thenReturn(COMPRESSED_EXTENSION);
        when(compression.getSplits(anyLong())).thenReturn(TEST_NUM_SPLITS);

        // when
        unitCompressor.compress(UNIT_PATH);

        // then
        verify(compression).compress(same(saveTestRDD), anyString(), any(JobConf.class));
    }

    @Test
    public void shouldContinueWhenSuccessFileNotExists() throws Exception {
        // given
        when(sparkContext.hadoopFile(eq(UNIT_PATH_NAME), eq(TextInputFormat.class),
                eq(LongWritable.class), eq(Text.class))).thenReturn(testRDD);
        when(testRDD.repartition(anyInt())).thenReturn(testRDD);
        when(fileSystem.globStatus(any(Path.class))).thenReturn(TEST_STATUSES);
        when(compression.getExtension()).thenReturn(COMPRESSED_EXTENSION);
        when(compression.getSplits(anyLong())).thenReturn(TEST_NUM_SPLITS);

        when(fileSystem.exists(any(Path.class))).thenAnswer(new Answer<Boolean>() {
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                return !isSuccessFile((Path) args[0]);
            }
        });

        // when
        unitCompressor.compress(UNIT_PATH);

        // then
        verify(compression).compress(same(saveTestRDD), anyString(), any(JobConf.class));
        verifyCleanup();
        verify(fileSystem).delete(not(eq(UNIT_PATH)), eq(true));
    }

    @Test
    public void shouldNotCleanUpWhenCountsAreDifferent() throws Exception {
        // given
        when(testRDD.count()).thenReturn(10L, 9L);
        when(sparkContext.hadoopFile(eq(UNIT_PATH_NAME), eq(TextInputFormat.class),
                eq(LongWritable.class), eq(Text.class))).thenReturn(testRDD);
        when(testRDD.repartition(anyInt())).thenReturn(testRDD);
        when(fileSystem.globStatus(any(Path.class))).thenReturn(TEST_STATUSES);
        when(compression.getExtension()).thenReturn(COMPRESSED_EXTENSION);
        when(compression.getSplits(anyLong())).thenReturn(TEST_NUM_SPLITS);

        // when
        try {
            unitCompressor.compress(UNIT_PATH);
            fail("Should fail with an InvalidCountsException");
        } catch (InvalidCountsException e) {

            //then
            verifyNotCleaningUp();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldNotCompressWhenNoFiles() throws IOException {
        // given
        when(fileSystem.globStatus(any(Path.class))).thenReturn(EMPTY_STATUSES);
        when(compression.getExtension()).thenReturn(COMPRESSED_EXTENSION);

        // when
        unitCompressor.compress(UNIT_PATH);

        // then
        verify(compression, never()).compress(any(JavaPairRDD.class), anyString(), any(JobConf.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldNotCompressWhenEmptyFiles() throws Exception {
        // given
        when(fileSystem.globStatus(any(Path.class))).thenReturn(EMPTY_FILES_STATUSES);
        when(compression.getExtension()).thenReturn(COMPRESSED_EXTENSION);

        // when
        unitCompressor.compress(UNIT_PATH);

        // then
        verify(compression, never()).compress(any(JavaPairRDD.class), anyString(), any(JobConf.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldNotCompressAlreadyCompressedFiles() throws Exception {
        // given
        when(fileSystem.globStatus(any(Path.class))).thenReturn(COMPRESSED_TEST_STATUSES);
        when(compression.getExtension()).thenReturn(COMPRESSED_EXTENSION);

        // when
        unitCompressor.compress(UNIT_PATH);

        // then
        verify(compression, never()).compress(any(JavaPairRDD.class), anyString(), any(JobConf.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldNotCompressWhenSuccessFileExists() throws Exception {
        // given
        when(fileSystem.globStatus(any(Path.class))).thenReturn(TEST_STATUSES);
        when(compression.getExtension()).thenReturn(COMPRESSED_EXTENSION);
        when(fileSystem.exists(any(Path.class))).thenReturn(true);

        // when
        unitCompressor.compress(UNIT_PATH);

        // then
        verify(compression, never()).compress(any(JavaPairRDD.class), anyString(), any(JobConf.class));
        verifyCleanup();
    }

    private void verifyCleanup() throws IOException {
        verifyFinalizationInteractions(1);
        ArgumentCaptor<Path> pathCaptor = ArgumentCaptor.forClass(Path.class);
        verify(fileSystem).delete(pathCaptor.capture(), eq(false));
        assertTrue(isSuccessFile(pathCaptor.getValue()));
    }

    private void verifyNotCleaningUp() throws IOException {
        verifyFinalizationInteractions(0);
    }

    private void verifyFinalizationInteractions(int invocations) throws IOException {
        verify(fileSystem, times(invocations)).delete(eq(UNIT_PATH), eq(true));
        final ArgumentCaptor<Path> fromPathCaptor = ArgumentCaptor.forClass(Path.class);
        final ArgumentCaptor<Path> toPathCaptor = ArgumentCaptor.forClass(Path.class);
        verify(fileSystem, times(invocations)).rename(fromPathCaptor.capture(), toPathCaptor.capture());
        if (invocations > 0) {
            assertTrue(fromPathCaptor.getValue().toString().contains(WORKING_PATH));
            assertTrue(toPathCaptor.getValue().toString().contains(UNIT_PATH.toString()));
        }
    }

    private boolean isSuccessFile(Path path) {
        return path.toString().endsWith("_SUCCESS");
    }

    private static final String COMPRESSED_EXTENSION = "ext";
    private static final String UNCOMPRESSED_EXTENSION = "other_extn";
    private static final String UNIT_NAME = "test_unit";
    private static final Path UNIT_PATH = new Path(UNIT_NAME);
    private static final String UNIT_PATH_NAME = "test_unit/*";
    private static final String FILE_NAME_TO_COMPRESS = "test_file." + UNCOMPRESSED_EXTENSION;
    private static final Path FILE_NAME_TO_COMPRESS_PATH = new Path(FILE_NAME_TO_COMPRESS);
    private static final FileStatus[] TEST_STATUSES = {fileStatusForPath(FILE_NAME_TO_COMPRESS_PATH)};
    private static final int TEST_NUM_SPLITS = 4;
    private static final FileStatus[] EMPTY_FILES_STATUSES = {fileStatusForEmptyFile(FILE_NAME_TO_COMPRESS_PATH)};

    private static final String COMPRESSED_FILE_NAME = "test_file." + COMPRESSED_EXTENSION;
    private static final Path COMPRESSED_PATH = new Path(COMPRESSED_FILE_NAME);
    private static final FileStatus[] COMPRESSED_TEST_STATUSES = {fileStatusForPath(COMPRESSED_PATH)};

    private static final FileStatus[] EMPTY_STATUSES = {};
    private static final String WORKING_PATH = "/tmp/compressor";


}
