package pl.allegro.tech.hadoop.compressor.compression;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

public class SnappyCompression implements Compression {

    private JavaSparkContext sparkContext;
    private long inputBlockSize;

    public SnappyCompression(FileSystem fileSystem, JavaSparkContext sparkContext) {
        this.sparkContext = sparkContext;
        this.inputBlockSize = fileSystem.getDefaultBlockSize(new Path("/")) * 2;
    }

    @Override
    public void compress(JavaRDD<String> content, String outputDir) throws IOException {
        content.saveAsTextFile(outputDir, SnappyCodec.class);
    }

    @Override
    public JavaRDD<String> decompress(String inputFile) throws IOException {
        return sparkContext.textFile(String.format("%s/*.%s", inputFile, getExtension()));
    }

    @Override
    public int getSplits(long size) {
        return (int) ((size + inputBlockSize - 1) / inputBlockSize);
    }

    @Override
    public String getExtension() {
        return "snappy";
    }
}
