package pl.allegro.tech.hadoop.compressor.compression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SnappyCompressionTest {

    private static final String OUTPUT_DIR = "test_dir";
    private static final String INPUT_FILE = "test_input_file";

    @Mock
    private FileSystem fileSystem;

    @Mock
    private JavaSparkContext sparkContext;

    @Mock
    private JavaRDD<String> content;

    private Compression snappyCompression;

    @Before
    public void setUp() throws Exception {
        when(fileSystem.getDefaultBlockSize(any(Path.class)))
                .thenReturn(256L);
        snappyCompression = new SnappyCompression(fileSystem, sparkContext);
    }

    @Test
    public void shouldCompressWithJsonCodec() throws Exception {
        //when
        snappyCompression.compress(content, OUTPUT_DIR);

        //then
        verify(content).saveAsTextFile(OUTPUT_DIR, SnappyCodec.class);
    }

    @Test
    public void shouldDecompressSnappyFiles() throws Exception {
        // when
        snappyCompression.decompress(INPUT_FILE);

        // then
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(sparkContext).textFile(captor.capture());

        assertTrue(captor.getValue().startsWith(INPUT_FILE));
        assertTrue(captor.getValue().endsWith(snappyCompression.getExtension()));

    }

    @Test
    public void shouldProvideProperNumberOfFileSplits() {
        assertEquals(2, snappyCompression.getSplits(1024L));
        assertEquals(1, snappyCompression.getSplits(128L));
    }
}