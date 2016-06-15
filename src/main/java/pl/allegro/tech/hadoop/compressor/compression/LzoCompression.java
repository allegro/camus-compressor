package pl.allegro.tech.hadoop.compressor.compression;

import com.hadoop.compression.lzo.LzoIndex;
import com.hadoop.compression.lzo.LzopCodec;
import pl.allegro.tech.hadoop.compressor.util.ExtensionAwareAvroOutputFormat;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

class LzoCompression<K, S, V, I extends InputFormat<K, V>, O extends OutputFormat<S, V>>
        extends AbstractCompression<K, S, V, I, O> {

    private final FileSystem fileSystem;

    public LzoCompression(JavaSparkContext sparkContext, FileSystem fileSystem, Class<K> keyClass, Class<S> saveClass,
                          Class<V> valueClass, Class<I> inputFormatClass, Class<O> outputFormatClass) {

        super(sparkContext, keyClass, saveClass, valueClass, inputFormatClass, outputFormatClass);
        this.fileSystem = fileSystem;
    }

    @Override
    protected void setupJobConf(JobConf jobConf) {
        jobConf.setBoolean(ExtensionAwareAvroOutputFormat.EXTENSION_FROM_CODEC_KEY, true);
        jobConf.setBoolean(MAPRED_COMPRESS_KEY, true);
        jobConf.set(COMPRESSION_CODEC_KEY, LzopCodec.class.getName());
        jobConf.set(COMPRESSION_TYPE_KEY, COMPRESSION_TYPE_BLOCK);
    }

    @Override
    public void compress(JavaPairRDD<S, V> content, String outputDir, JobConf jobConf) throws IOException {
        super.compress(content, outputDir, jobConf);

        for (FileStatus fileStatus : fileSystem.listStatus(new Path(outputDir), new GlobFilter("*.lzo"))) {
            LzoIndex.createIndex(fileSystem, fileStatus.getPath());
        }
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
