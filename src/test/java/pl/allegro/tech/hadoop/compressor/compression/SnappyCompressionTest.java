package pl.allegro.tech.hadoop.compressor.compression;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import pl.allegro.tech.hadoop.compressor.option.CompressionFormat;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static pl.allegro.tech.hadoop.compressor.Utils.checkDecompress;

@RunWith(MockitoJUnitRunner.class)
public class SnappyCompressionTest {

    private static final String OUTPUT_DIR = "test_dir";
    private static final String INPUT_FILE = "test_input_file";

    @Mock
    private FileSystem fileSystem;

    @Mock
    private JavaSparkContext sparkContext;

    @Mock
    private JavaPairRDD<NullWritable, Text> content;

    private JobConf jobConf = new JobConf();

    private Compression<LongWritable, NullWritable, Text> snappyCompression;

    @Before
    public void setUp() throws Exception {
        when(fileSystem.getDefaultBlockSize(any(Path.class)))
                .thenReturn(256L);
        snappyCompression = CompressionBuilder.forSparkContext(sparkContext)
                .onFileSystem(fileSystem)
                .withCompressorOfType(CompressionFormat.SNAPPY)
                .forJsonFiles();
    }

    @Test
    public void shouldCompressWithJsonCodec() throws Exception {
        //when
        snappyCompression.compress(content, OUTPUT_DIR, jobConf);

        //then
        verify(content).saveAsHadoopFile(OUTPUT_DIR, NullWritable.class, Text.class, TextOutputFormat.class, jobConf);
    }

    @Test
    public void shouldDecompressSnappyFiles() throws Exception {
        checkDecompress(INPUT_FILE, snappyCompression, sparkContext);
    }

    @Test
    public void shouldProvideProperNumberOfFileSplits() {
        assertEquals(2, snappyCompression.getSplits(1024L));
        assertEquals(1, snappyCompression.getSplits(128L));
    }
}