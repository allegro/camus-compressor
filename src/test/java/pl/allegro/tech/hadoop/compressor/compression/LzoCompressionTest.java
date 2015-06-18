package pl.allegro.tech.hadoop.compressor.compression;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.hadoop.compression.lzo.LzoCodec;
import com.hadoop.compression.lzo.LzopCodec;

@RunWith(MockitoJUnitRunner.class)
public class LzoCompressionTest {

    private static final String OUTPUT_DIR_NAME = "output_dir_test";
    private static final Path OUTPUT_PATH = new Path(OUTPUT_DIR_NAME);
    private static final Path COMPRESSED_FILE_PATH = OUTPUT_PATH.suffix("file.lzo");
    private static final Path SECOND_COMPRESSED_FILE_PATH = OUTPUT_PATH.suffix("file2.lzo");
    private static final FileStatus[] TEST_STATUSES = {fileStatusForPath(COMPRESSED_FILE_PATH),
                                                            fileStatusForPath(SECOND_COMPRESSED_FILE_PATH)};
    private static final String CODEC_SUBSTRING = LzoCodec.class.getName();

    public static final Logger logger = Logger.getLogger(LzoCompressionTest.class);
    private static final String INPUT_FILE_NAME = "test_file";

    @Mock
    private Configuration configuration;

    @Mock
    private JavaSparkContext sparkContext;

    @Mock
    private FileSystem fileSystem;

    @Mock
    private JavaRDD<String> content;

    private LzoCompression lzoCompression;

    @Before
    public void setUp() {
        lzoCompression = new LzoCompression(sparkContext, fileSystem);
    }

    @Test
    public void shouldCompressWithLzoCodec() throws Exception {
        // given
        when(configuration.get(eq("io.compression.codecs"))).thenReturn(CODEC_SUBSTRING);
        when(configuration.getBoolean(eq("hadoop.native.lib"), anyBoolean())).thenReturn(true);

        final Field field = LzoCodec.class.getDeclaredField("nativeLzoLoaded");
        field.setAccessible(true);
        field.setBoolean(LzoCodec.class, true);

        when(configuration.getClassByName(CODEC_SUBSTRING)).thenAnswer(new Answer<Class<?>>() {
            @Override
            public Class<?> answer(InvocationOnMock invocation) throws Throwable {
                return LzopCodec.class;
            }
        });

        when(fileSystem.getConf()).thenReturn(configuration);
        when(fileSystem.listStatus(eq(OUTPUT_PATH), any(GlobFilter.class))).thenReturn(TEST_STATUSES);

        // when
        try {
            lzoCompression.compress(content, OUTPUT_DIR_NAME);
        } catch (UnsatisfiedLinkError ex) {
            logger.warn("native lzo library not loaded (acceptable in unit tests)");
        }

        // then
        verify(content).saveAsTextFile(OUTPUT_DIR_NAME, LzopCodec.class);
    }

    @Test
    public void shouldDecompressLzoFiles() throws Exception {
        // when
        lzoCompression.decompress(INPUT_FILE_NAME);

        // then
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(sparkContext).textFile(captor.capture());
        assertTrue(captor.getValue().startsWith(INPUT_FILE_NAME));
        assertTrue(captor.getValue().endsWith(lzoCompression.getExtension()));

    }

    private static final FileStatus fileStatusForPath(Path path) {
        return new FileStatus(10L, true, 3, 1024L, 100L, path);
    }
}