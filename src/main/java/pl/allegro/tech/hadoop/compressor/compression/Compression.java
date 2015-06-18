package pl.allegro.tech.hadoop.compressor.compression;

import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;

public interface Compression {

    void compress(JavaRDD<String> content, String outputDir) throws IOException;

    JavaRDD<String> decompress(String inputPath) throws IOException;
    
    int getSplits(long size);
    
    String getExtension();

}
