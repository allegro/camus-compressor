package pl.allegro.tech.hadoop.compressor.compression;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.spark.api.java.JavaSparkContext;

class NoneCompression<K, S, V, I extends InputFormat<K, V>, O extends OutputFormat<S, V>>
        extends AbstractCompression<K, S, V, I, O> {

    private long inputBlockSize;

    public NoneCompression(JavaSparkContext sparkContext, FileSystem fileSystem, Class<K> keyClass, Class<S> sClass,
                           Class<V> valueClass, Class<I> inputFormatClass, Class<O> outputFormatClass) {

        super(sparkContext, keyClass, sClass, valueClass, inputFormatClass, outputFormatClass);
        this.inputBlockSize = fileSystem.getDefaultBlockSize(new Path("/"));
    }

    @Override
    public int getSplits(long size) {
        return (int) ((size + inputBlockSize - 1) / inputBlockSize);
    }

    @Override
    public String getExtension() {
        return "";
    }

    @Override
    protected void setupJobConf(JobConf jobConf) {
        jobConf.setBoolean(MAPRED_COMPRESS_KEY, false);
    }
}
