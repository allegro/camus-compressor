package pl.allegro.tech.hadoop.compressor.mode.unit;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import pl.allegro.tech.hadoop.compressor.compression.Compression;
import pl.allegro.tech.hadoop.compressor.schema.SchemaRepository;
import pl.allegro.tech.hadoop.compressor.util.InputAnalyser;

import java.io.IOException;

public class AvroUnitCompressor extends UnitCompressor {

    private final JavaSparkContext sparkContext;
    private final SchemaRepository schemaRepository;
    private final Compression<AvroWrapper<GenericRecord>, AvroWrapper<GenericRecord>, NullWritable> compression;

    public AvroUnitCompressor(JavaSparkContext sparkContext, FileSystem fileSystem, InputAnalyser inputAnalyser,
                              SchemaRepository schemaRepository,
                              Compression<AvroWrapper<GenericRecord>, AvroWrapper<GenericRecord>, NullWritable> compression) {

        super(fileSystem, inputAnalyser);
        this.sparkContext = sparkContext;
        this.schemaRepository = schemaRepository;
        this.compression = compression;
    }

    @Override
    protected void repartition(String inputPath, String outputDir, String jobGroup, int inputSplits)
            throws IOException {

        final JavaPairRDD<AvroWrapper<GenericRecord>, NullWritable> repartitionedRDD = compression
                .openUncompressed(inputPath)
                .repartition(inputSplits);

        final JobConf jobConf = new JobConf(sparkContext.hadoopConfiguration());

        AvroJob.setOutputSchema(jobConf, schemaRepository.findLatestSchema(inputPath));

        sparkContext.setJobGroup("compression", jobGroup);
        compression.compress(repartitionedRDD, outputDir, jobConf);
    }
}