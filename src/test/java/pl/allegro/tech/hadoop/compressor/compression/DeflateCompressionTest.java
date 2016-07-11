package pl.allegro.tech.hadoop.compressor.compression;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static pl.allegro.tech.hadoop.compressor.Utils.checkDecompress;
import static pl.allegro.tech.hadoop.compressor.Utils.fileStatusForPath;


@RunWith(MockitoJUnitRunner.class)
public class DeflateCompressionTest {

    private static final String CODEC = DeflateCodec.class.getName();
    private static final String OUTPUT_DIR_NAME = "output_dir_name";
    private static final Path OUTPUT_PATH = new Path(OUTPUT_DIR_NAME);
    private static final Path COMPRESSED_FILE_PATH = OUTPUT_PATH.suffix("file.deflate");
    private static final Path SECOND_COMPRESSED_FILE_PATH = OUTPUT_PATH.suffix("file2.deflate");
    private static final String INPUT_FILE = "test_file";
    private static final org.apache.hadoop.fs.FileStatus[] TEST_STATUSES = new FileStatus[] {
            fileStatusForPath(COMPRESSED_FILE_PATH), fileStatusForPath(SECOND_COMPRESSED_FILE_PATH)
    };

    @Mock
    private Configuration configuration;

    @Mock
    private JavaSparkContext sparkContext;

    @Mock
    private FileSystem fileSystem;

    @Mock
    private JavaPairRDD<NullWritable, Text> content;

    private Compression<LongWritable, NullWritable, Text> deflateCompression;
    private JobConf jobConf;

    @Before
    public void setUp() throws Exception {
        deflateCompression = CompressionBuilder.forSparkContext(sparkContext)
                .onFileSystem(fileSystem)
                .withCompressorOfType(CompressionFormat.DEFLATE)
                .forJsonFiles();
        jobConf = new JobConf();
    }

    @Test
    public void shouldCompressWithDeflateCodec() throws Exception {
        // given
        when(configuration.get("io.compression.codecs")).thenReturn(CODEC);
        when(configuration.getBoolean(eq("hadoop.native.lib"), anyBoolean())).thenReturn(true);

        when(configuration.getClassByName(CODEC)).thenAnswer(new Answer<Class<?>>() {
            @Override
            public Class<?> answer(InvocationOnMock invocation) throws Throwable {
                return DeflateCodec.class;
            }
        });

        when(fileSystem.getConf()).thenReturn(configuration);
        when(fileSystem.listStatus(eq(OUTPUT_PATH), any(GlobFilter.class))).thenReturn(TEST_STATUSES);

        // when
        deflateCompression.compress(content, OUTPUT_DIR_NAME, jobConf);

        // then
        verify(content).saveAsHadoopFile(eq(OUTPUT_DIR_NAME), eq(NullWritable.class), eq(Text.class),
                eq(TextOutputFormat.class), any(JobConf.class));
    }

    @Test
    public void shouldDecompressDeflate() throws Exception {
        checkDecompress(INPUT_FILE, deflateCompression, sparkContext);
    }
}