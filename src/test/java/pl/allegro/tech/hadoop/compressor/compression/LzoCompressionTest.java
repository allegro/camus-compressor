package pl.allegro.tech.hadoop.compressor.compression;

import com.hadoop.compression.lzo.LzoCodec;
import com.hadoop.compression.lzo.LzopCodec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import pl.allegro.tech.hadoop.compressor.option.CompressionFormat;

import java.lang.reflect.Field;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static pl.allegro.tech.hadoop.compressor.Utils.checkDecompress;
import static pl.allegro.tech.hadoop.compressor.Utils.fileStatusForPath;

@RunWith(MockitoJUnitRunner.class)
public class LzoCompressionTest {

    private static final Logger logger = Logger.getLogger(LzoCompressionTest.class);
    private static final String OUTPUT_DIR_NAME = "output_dir_test";
    private static final Path OUTPUT_PATH = new Path(OUTPUT_DIR_NAME);
    private static final Path COMPRESSED_FILE_PATH = OUTPUT_PATH.suffix("file.lzo");
    private static final Path SECOND_COMPRESSED_FILE_PATH = OUTPUT_PATH.suffix("file2.lzo");
    private static final FileStatus[] TEST_STATUSES = {fileStatusForPath(COMPRESSED_FILE_PATH),
                                                            fileStatusForPath(SECOND_COMPRESSED_FILE_PATH)};
    private static final String CODEC_SUBSTRING = LzoCodec.class.getName();

    private static final String INPUT_FILE = "test_file";

    @Mock
    private Configuration configuration;

    @Mock
    private JavaSparkContext sparkContext;

    @Mock
    private FileSystem fileSystem;

    @Mock
    private JavaPairRDD<NullWritable, Text> content;

    private Compression<LongWritable, NullWritable, Text> lzoCompression;
    private JobConf jobConf;

    @Before
    public void setUp() {
        lzoCompression = CompressionBuilder.forSparkContext(sparkContext)
                .onFileSystem(fileSystem)
                .withCompressorOfType(CompressionFormat.LZO)
                .forJsonFiles();
        jobConf = new JobConf();
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
            lzoCompression.compress(content, OUTPUT_DIR_NAME, jobConf);
        } catch (UnsatisfiedLinkError ex) {
            logger.warn("native lzo library not loaded (acceptable in unit tests)");
        }

        // then
        verify(content).saveAsHadoopFile(OUTPUT_DIR_NAME, NullWritable.class, Text.class, TextOutputFormat.class, jobConf);
        assertEquals(jobConf.get(AbstractCompression.COMPRESSION_CODEC_KEY), LzopCodec.class.getName());
    }

    @Test
    public void shouldDecompressLzoFiles() throws Exception {
        checkDecompress(INPUT_FILE, lzoCompression, sparkContext);
    }
}
