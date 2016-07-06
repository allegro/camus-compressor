package pl.allegro.tech.hadoop.compressor.mode.unit;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import pl.allegro.tech.hadoop.compressor.compression.Compression;
import pl.allegro.tech.hadoop.compressor.schema.SchemaRepository;
import pl.allegro.tech.hadoop.compressor.util.InputAnalyser;
import scala.Tuple2;

import java.io.IOException;

public class AvroUnitCompressor extends UnitCompressor {

    private static final Logger logger = Logger.getLogger(AvroUnitCompressor.class);

    private final JavaSparkContext sparkContext;
    private final SchemaRepository schemaRepository;
    private final Compression<AvroWrapper<GenericRecord>, AvroWrapper<GenericRecord>, NullWritable> compression;

    public AvroUnitCompressor(JavaSparkContext sparkContext, FileSystem fileSystem, InputAnalyser inputAnalyser,
                              String workingPath,
                              SchemaRepository schemaRepository,
                              Compression<AvroWrapper<GenericRecord>, AvroWrapper<GenericRecord>, NullWritable> compression) {

        super(fileSystem, inputAnalyser, workingPath);
        this.sparkContext = sparkContext;
        this.schemaRepository = schemaRepository;
        this.compression = compression;
    }

    @Override
    protected void repartition(String inputPath, String outputDir, String jobGroup, int inputSplits)
            throws IOException {

        final JobConf jobConf = new JobConf(sparkContext.hadoopConfiguration());

        final Schema schema = schemaRepository.findLatestSchema(inputPath);
        AvroJob.setOutputSchema(jobConf, schema);
        final JavaPairRDD<AvroWrapper<GenericRecord>, NullWritable> repartitionedRDD = compression
                .openUncompressed(inputPath)
                .mapToPair(new SchemaEvolveMap(schema.toString()))
                .repartition(inputSplits);

        logger.info("Compressing " + inputPath + " avro with schema " + schema.toString());

        sparkContext.setJobGroup("compression", jobGroup);
        compression.compress(repartitionedRDD, outputDir, jobConf);
    }

    private static class SchemaEvolveMap implements PairFunction<Tuple2<AvroWrapper<GenericRecord>, NullWritable>, AvroWrapper<GenericRecord>, NullWritable> {

        private static final long serialVersionUID = -89036365162892418L;

        private final String schema;

        public SchemaEvolveMap(String schema) {
            this.schema = schema;
        }

        @Override
        public Tuple2<AvroWrapper<GenericRecord>, NullWritable> call(Tuple2<AvroWrapper<GenericRecord>, NullWritable> wrapperPair) throws Exception {
            final AvroWrapper<GenericRecord> outWrapper = new AvroWrapper<>();
            outWrapper.datum(new GenericData.Record(new Schema.Parser().parse(schema)));
            projectData(wrapperPair._1().datum(), outWrapper.datum());
            return new Tuple2<>(outWrapper, NullWritable.get());
        }

        private void projectData(GenericRecord source, GenericRecord target) {
            for (Schema.Field fld : target.getSchema().getFields()) {
                if (fld.schema().getType() == Schema.Type.UNION) {
                    Object obj = source.get(fld.name());
                    Schema sourceSchema = GenericData.get().induce(obj);
                    if (sourceSchema.getType() == Schema.Type.RECORD) {
                        for (Schema type : fld.schema().getTypes()) {
                            if (type.getFullName().equals(sourceSchema.getFullName())) {
                                GenericRecord record = new GenericData.Record(type);
                                target.put(fld.name(), record);
                                projectData((GenericRecord) obj, record);
                                break;
                            }
                        }
                    } else {
                        target.put(fld.name(), source.get(fld.name()));
                    }
                } else if (fld.schema().getType() == Schema.Type.RECORD) {
                    GenericRecord record = (GenericRecord) target.get(fld.name());

                    if (record == null) {
                        record = new GenericData.Record(fld.schema());
                        target.put(fld.name(), record);
                    }

                    projectData((GenericRecord) source.get(fld.name()), record);
                } else {
                    target.put(fld.name(), source.get(fld.name()));
                }
            }
        }
    }
}