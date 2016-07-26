package pl.allegro.tech.hadoop.compressor.mode.unit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import pl.allegro.tech.hadoop.compressor.compression.Compression;
import pl.allegro.tech.hadoop.compressor.util.InputAnalyser;
import scala.Tuple2;

import java.io.IOException;

public class JsonUnitCompressor extends UnitCompressor<LongWritable, Text> {

    private final JavaSparkContext context;
    private final Compression<LongWritable, NullWritable, Text> compression;

    public JsonUnitCompressor(JavaSparkContext context, FileSystem fileSystem,
                              String workingPath, String backupDir,
                              Compression<LongWritable, NullWritable, Text> compression, InputAnalyser inputAnalyser,
                              boolean calculateCounts) {

        super(fileSystem, inputAnalyser, workingPath, backupDir, calculateCounts);
        this.compression = compression;
        this.context = context;
    }

    @Override
    protected JobConf createJobConf(String path, String schemaReaderPath) {
        JobConf jobConf = new JobConf(context.hadoopConfiguration());
        FileInputFormat.setInputPaths(jobConf, path);
        return jobConf;
    }

    @Override
    protected JavaPairRDD<LongWritable, Text> fetchRDD(JobConf jobConf) throws IOException {
        return compression.openUncompressed(jobConf);
    }

    @Override
    protected long countOutputDir(JavaPairRDD rdd) throws IOException {
        return rdd.count();
    }

    @Override
    protected void repartition(JavaPairRDD<LongWritable, Text> rdd, String outputPath, String jobGroup, JobConf jobConf, int inputSplits) throws IOException {
        final JavaPairRDD<NullWritable, Text> repartitionedRDD = rdd
                .repartition(inputSplits)
                .mapToPair(new RemoveKeyFunction());

        context.setJobGroup("compression", jobGroup);
        compression.compress(repartitionedRDD, outputPath, jobConf);
    }

    private static class RemoveKeyFunction implements PairFunction<Tuple2<LongWritable, Text>, NullWritable, Text> {

        private static final long serialVersionUID = -2294870359967137820L;

        @Override
        public Tuple2<NullWritable, Text> call(Tuple2<LongWritable, Text> keyedTuple) throws Exception {
            return new Tuple2<>(NullWritable.get(), keyedTuple._2());
        }
    }
}
