package pl.allegro.tech.hadoop.compressor;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.spark.api.java.JavaSparkContext;
import org.mockito.ArgumentCaptor;
import pl.allegro.tech.hadoop.compressor.compression.Compression;

import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;

public class Utils {

    public static FileStatus fileStatusForPath(Path path) {
        return new FileStatus(10L, true, 3, 1024L, 100L, path);
    }

    public static FileStatus fileStatusForEmptyFile(Path path) {
        return new FileStatus(0L, true, 3, 1024L, 100L, path);
    }

    public static void checkDecompress(String inputFile, Compression compression, JavaSparkContext sparkContext) throws IOException {
        // when
        compression.decompress(inputFile);

        // then
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(sparkContext).hadoopFile(captor.capture(), eq(TextInputFormat.class), eq(LongWritable.class),
                eq(Text.class));
        assertTrue(captor.getValue().startsWith(inputFile));
        assertTrue(captor.getValue().endsWith(compression.getExtension()));
    }
}
