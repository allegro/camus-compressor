package pl.allegro.tech.hadoop.compressor.compression;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class NoneCompression implements Compression {

    private JavaSparkContext sparkContext;
    private long inputBlockSize;

    public NoneCompression(FileSystem fileSystem, JavaSparkContext sparkContext) {
        this.sparkContext = sparkContext;
        this.inputBlockSize = fileSystem.getDefaultBlockSize(new Path("/"));
    }

    @Override
    public void compress(JavaRDD<String> content, String outputDir) throws IOException {
        content.saveAsTextFile(outputDir);
    }

    @Override
    public JavaRDD<String> decompress(String inputFile) throws IOException {
        return sparkContext.textFile(String.format("%s/*", inputFile));
    }

    @Override
    public int getSplits(long size) {
        return (int) ((size + inputBlockSize - 1) / inputBlockSize);
    }

    @Override
    public String getExtension() {
        return "";
    }
}
