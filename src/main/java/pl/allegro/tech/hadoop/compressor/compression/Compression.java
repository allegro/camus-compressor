package pl.allegro.tech.hadoop.compressor.compression;

import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaPairRDD;

import java.io.IOException;

public interface Compression<K, S, V> {

    void compress(JavaPairRDD<S, V> content, String outputDir, JobConf jobConf) throws IOException;

    JavaPairRDD<K, V> decompress(String inputPath) throws IOException;

    JavaPairRDD<K, V> openUncompressed(String inputPath) throws IOException;

    JavaPairRDD<K, V> openUncompressed(JobConf jobConf) throws IOException;

    int getSplits(long size);

    String getExtension();

}
