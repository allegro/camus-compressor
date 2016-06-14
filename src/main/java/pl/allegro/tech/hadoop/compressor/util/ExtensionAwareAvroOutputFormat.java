package pl.allegro.tech.hadoop.compressor.util;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.hadoop.file.HadoopCodecFactory;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;

import static org.apache.avro.file.CodecFactory.DEFAULT_DEFLATE_LEVEL;
import static org.apache.avro.file.CodecFactory.DEFAULT_XZ_LEVEL;
import static org.apache.avro.file.DataFileConstants.DEFAULT_SYNC_INTERVAL;
import static org.apache.avro.file.DataFileConstants.DEFLATE_CODEC;
import static org.apache.avro.file.DataFileConstants.XZ_CODEC;

public class ExtensionAwareAvroOutputFormat<T> extends AvroOutputFormat<T> {


    public static final String EXTENSION_OVERRIDE_KEY = "avro.output.extension.override";

    static CodecFactory getCodecFactory(JobConf job) {
        CodecFactory factory = null;

        if (FileOutputFormat.getCompressOutput(job)) {
            int deflateLevel = job.getInt(DEFLATE_LEVEL_KEY, DEFAULT_DEFLATE_LEVEL);
            int xzLevel = job.getInt(XZ_LEVEL_KEY, DEFAULT_XZ_LEVEL);
            String codecName = job.get(AvroJob.OUTPUT_CODEC);

            if (codecName == null) {
                String codecClassName = job.get("mapred.output.compression.codec", null);
                String avroCodecName = HadoopCodecFactory.getAvroCodecName(codecClassName);
                if ( codecClassName != null && avroCodecName != null){
                    factory = HadoopCodecFactory.fromHadoopString(codecClassName);
                    job.set(AvroJob.OUTPUT_CODEC , avroCodecName);
                    return factory;
                } else {
                    return CodecFactory.deflateCodec(deflateLevel);
                }
            } else {
                if ( codecName.equals(DEFLATE_CODEC)) {
                    factory = CodecFactory.deflateCodec(deflateLevel);
                } else if ( codecName.equals(XZ_CODEC)) {
                    factory = CodecFactory.xzCodec(xzLevel);
                } else {
                    factory = CodecFactory.fromString(codecName);
                }
            }
        }

        return factory;
    }

    static <T> void configureDataFileWriter(DataFileWriter<T> writer,
                                            JobConf job) throws UnsupportedEncodingException {

        CodecFactory factory = getCodecFactory(job);

        if (factory != null) {
            writer.setCodec(factory);
        }

        writer.setSyncInterval(job.getInt(SYNC_INTERVAL_KEY, DEFAULT_SYNC_INTERVAL));

        // copy metadata from job
        for (Map.Entry<String,String> e : job) {
            if (e.getKey().startsWith(AvroJob.TEXT_PREFIX))
                writer.setMeta(e.getKey().substring(AvroJob.TEXT_PREFIX.length()),
                        e.getValue());
            if (e.getKey().startsWith(AvroJob.BINARY_PREFIX))
                writer.setMeta(e.getKey().substring(AvroJob.BINARY_PREFIX.length()),
                        URLDecoder.decode(e.getValue(), "ISO-8859-1")
                                .getBytes("ISO-8859-1"));
        }
    }

    @Override
    public RecordWriter<AvroWrapper<T>, NullWritable> getRecordWriter(FileSystem ignore, JobConf job, String name,
                                                                      Progressable prog) throws IOException {
        boolean isMapOnly = job.getNumReduceTasks() == 0;
        Schema schema = isMapOnly
                ? AvroJob.getMapOutputSchema(job)
                : AvroJob.getOutputSchema(job);
        GenericData dataModel = AvroJob.createDataModel(job);

        @SuppressWarnings("unchecked")
        final DataFileWriter<T> writer =
                new DataFileWriter<>(dataModel.createDatumWriter(null));

        ExtensionAwareAvroOutputFormat.configureDataFileWriter(writer, job);

        final String ext = job.get(EXTENSION_OVERRIDE_KEY, EXT);
        Path path = FileOutputFormat.getTaskOutputPath(job, name + ext);
        writer.create(schema, path.getFileSystem(job).create(path));

        return new RecordWriter<AvroWrapper<T>, NullWritable>() {
            public void write(AvroWrapper<T> wrapper, NullWritable ignore)
                    throws IOException {
                writer.append(wrapper.datum());
            }
            public void close(Reporter reporter) throws IOException {
                writer.close();
            }
        };

    }
}
