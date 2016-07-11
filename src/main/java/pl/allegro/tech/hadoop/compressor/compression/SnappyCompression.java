package pl.allegro.tech.hadoop.compressor.compression;

import pl.allegro.tech.hadoop.compressor.util.ExtensionAwareAvroOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

class SnappyCompression<K, S, V, I extends InputFormat<K, V>, O extends OutputFormat<S, V>>
        extends AbstractCompression<K, S, V, I, O> {

    private static final Logger logger = Logger.getLogger(SnappyCompression.class);

    private long inputBlockSize;

    public SnappyCompression(JavaSparkContext sparkContext, FileSystem fileSystem, Class<K> keyClass, Class<S> sClass,
                             Class<V> valueClass, Class<I> inputFormatClass, Class<O> outputFormatClass) {

        super(sparkContext, keyClass, sClass, valueClass, inputFormatClass, outputFormatClass);
        this.inputBlockSize = fileSystem.getDefaultBlockSize(new Path("/")) * 2;
        logger.warn("BlockSize = " + this.inputBlockSize);
    }

    @Override
    public int getSplits(long size) {
        return (int) ((size + inputBlockSize - 1) / inputBlockSize);
    }

    @Override
    public String getExtension() {
        return "snappy";
    }

    @Override
    protected void setupJobConf(JobConf jobConf) {
        jobConf.setCompressMapOutput(true);
        jobConf.setBoolean(ExtensionAwareAvroOutputFormat.EXTENSION_FROM_CODEC_KEY, true);
        jobConf.set("mapred.output.compress", "true");
        jobConf.setMapOutputCompressorClass(SnappyCodec.class);
        jobConf.set("mapred.output.compression.codec", SnappyCodec.class.getCanonicalName());
        jobConf.set("mapred.output.compression.type", SequenceFile.CompressionType.BLOCK.toString());
    }
}
