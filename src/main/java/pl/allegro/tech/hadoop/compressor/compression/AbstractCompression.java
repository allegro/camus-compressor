package pl.allegro.tech.hadoop.compressor.compression;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

public abstract class AbstractCompression<K, S, V, I extends InputFormat<K, V>, O extends OutputFormat<S, V>>
        implements Compression<K, S, V> {

    protected static final String MAPRED_COMPRESS_KEY = "mapred.output.compress";
    protected static final String COMPRESSION_CODEC_KEY = "mapred.output.compression.codec";
    protected static final String COMPRESSION_TYPE_KEY = "mapred.output.compression.type";
    protected static final String COMPRESSION_TYPE_BLOCK = "BLOCK";

    protected final JavaSparkContext sparkContext;
    protected final Class<K> kClass;
    protected final Class<S> sClass;
    protected final Class<V> vClass;
    protected final Class<I> iClass;
    protected final Class<O> oClass;

    AbstractCompression(JavaSparkContext sparkContext, Class<K> kClass, Class<S> sClass, Class<V> vClass,
                        Class<I> iClass, Class<O> oClass) {

        this.sparkContext = sparkContext;
        this.kClass = kClass;
        this.sClass = sClass;
        this.vClass = vClass;
        this.iClass = iClass;
        this.oClass = oClass;
    }

    public void compress(JavaPairRDD<S, V> content, String outputDir, JobConf jobConf)
            throws IOException {

        setupJobConf(jobConf);
        content.saveAsHadoopFile(outputDir, sClass, vClass, oClass, jobConf);
    }

    @Override
    public JavaPairRDD<K, V> decompress(String inputPath) throws IOException {
        return openUncompressed(String.format("%s/*.%s", inputPath, getExtension()));
    }

    @Override
    public JavaPairRDD<K, V> openUncompressed(String inputPath) throws IOException {
        return sparkContext.hadoopFile(inputPath, iClass, kClass, vClass);
    }

    @Override
    public JavaPairRDD<K, V> openUncompressed(JobConf jobConf) throws IOException {
        return sparkContext.hadoopRDD(jobConf, iClass, kClass, vClass);
    }

    protected void setupJobConf(JobConf jobConf) {
        jobConf.setBoolean(MAPRED_COMPRESS_KEY, false);
    }
}
