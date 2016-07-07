package pl.allegro.tech.hadoop.compressor.compression;

import pl.allegro.tech.hadoop.compressor.util.ExtensionAwareAvroOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

class DeflateCompression<K, S, V, I extends InputFormat<K, V>, O extends OutputFormat<S, V>>
    extends AbstractCompression<K, S, V, I, O> {

    public static final Logger logger = Logger.getLogger(DeflateCompression.class);

    private long inputBlockSize;

    public DeflateCompression(JavaSparkContext sparkContext, FileSystem fileSystem,
                              Class<K> kClass, Class<S> sClass, Class<V> vClass, Class<I> iClass, Class<O> oClass) {

        super(sparkContext, kClass, sClass, vClass, iClass, oClass);
        this.inputBlockSize = fileSystem.getDefaultBlockSize(new Path("/")) * 2;
        logger.warn("BlockSize = " + this.inputBlockSize);
    }

    @Override
    protected void setupJobConf(JobConf jobConf) {
        jobConf.setBoolean(ExtensionAwareAvroOutputFormat.EXTENSION_FROM_CODEC_KEY, true);
        jobConf.setBoolean(MAPRED_COMPRESS_KEY, true);
        jobConf.set(COMPRESSION_CODEC_KEY, DeflateCodec.class.getName());
        jobConf.set(COMPRESSION_TYPE_KEY, COMPRESSION_TYPE_BLOCK);
    }

    @Override
    public int getSplits(long size) {
        return (int) ((size + inputBlockSize - 1) / inputBlockSize);
    }

    @Override
    public String getExtension() {
        return "deflate";
    }
}
