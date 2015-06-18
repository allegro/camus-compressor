package pl.allegro.tech.hadoop.compressor.compression;

import com.hadoop.compression.lzo.LzoIndex;
import com.hadoop.compression.lzo.LzopCodec;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

public class LzoCompression implements Compression {
    private final JavaSparkContext sparkContext;
    private final FileSystem fileSystem;

    public LzoCompression(JavaSparkContext sparkContext, FileSystem fileSystem) {
        this.sparkContext = sparkContext;
        this.fileSystem = fileSystem;
    }

    @Override
    public void compress(JavaRDD<String> content, String outputDir) throws IOException {
        content.saveAsTextFile(outputDir, LzopCodec.class);

        for (FileStatus fileStatus : fileSystem.listStatus(new Path(outputDir), new GlobFilter("*.lzo"))) {
            LzoIndex.createIndex(fileSystem, fileStatus.getPath());
        }
    }

    @Override
    public JavaRDD<String> decompress(String inputFile) throws IOException {
        return sparkContext.textFile(String.format("%s/*.%s", inputFile, getExtension()));
    }

    @Override
    public int getSplits(long size) {
        return 1;
    }

    @Override
    public String getExtension() {
        return "lzo";
    }
}
