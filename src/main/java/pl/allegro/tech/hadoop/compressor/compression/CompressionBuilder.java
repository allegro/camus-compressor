package pl.allegro.tech.hadoop.compressor.compression;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.api.java.JavaSparkContext;
import pl.allegro.tech.hadoop.compressor.AvroInputFormatWithReaderSchema;
import pl.allegro.tech.hadoop.compressor.option.CompressionFormat;
import pl.allegro.tech.hadoop.compressor.util.ExtensionAwareAvroOutputFormat;

public class CompressionBuilder {

    private final JavaSparkContext sparkContext;
    private FileSystem fileSystem;
    private CompressionFormat compressionFormat;

    private static final Class<NullWritable> avroValueClass = NullWritable.class;
    @SuppressWarnings("unchecked")
    private static final  Class<AvroWrapper<GenericRecord>> avroKeyClass =
            (Class<AvroWrapper<GenericRecord>>) (Class<?>) AvroWrapper.class;
    @SuppressWarnings("unchecked")
    private static final Class<AvroInputFormatWithReaderSchema<GenericRecord>> avroInputFormat =
            (Class<AvroInputFormatWithReaderSchema<GenericRecord>>) (Class<?>) AvroInputFormatWithReaderSchema.class;
    @SuppressWarnings("unchecked")
    private static final Class<ExtensionAwareAvroOutputFormat<GenericRecord>> avroOutputFormat =
            (Class<ExtensionAwareAvroOutputFormat<GenericRecord>>) (Class<?>) ExtensionAwareAvroOutputFormat.class;

    private static final Class<Text> jsonValueClass = Text.class;
    private static final Class<LongWritable> jsonKeyClass = LongWritable.class;
    private static final Class<NullWritable> jsonSaveKeyClass = NullWritable.class;
    private static final Class<TextInputFormat> jsonInputFormat = TextInputFormat.class;

    @SuppressWarnings("unchecked")
    private static final Class<TextOutputFormat> jsonOutputFormat = TextOutputFormat.class;

    private CompressionBuilder(JavaSparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }

    public static CompressionBuilder forSparkContext(JavaSparkContext sparkContext) {
        return new CompressionBuilder(sparkContext);
    }

    public CompressorTypeBuilder onFileSystem(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
        return new CompressorTypeBuilder();
    }


    // nested classes are used for easy chaining Builder's methods in IDE - pretty public API
    public class CompressorTypeBuilder {

        private CompressorTypeBuilder() { }

        public CompressorInputFileFormatBuilder withCompressorOfType(CompressionFormat format) {
            compressionFormat = format;
            return new CompressorInputFileFormatBuilder();
        }
    }

    public class CompressorInputFileFormatBuilder {

        private CompressorInputFileFormatBuilder() { }

        public Compression<AvroWrapper<GenericRecord>, AvroWrapper<GenericRecord>, NullWritable> forAvroFiles() {

            if (CompressionFormat.DEFLATE.equals(compressionFormat)) {
                return new DeflateCompression<>(sparkContext, fileSystem, avroKeyClass, avroKeyClass, avroValueClass,
                        avroInputFormat, avroOutputFormat);
            } else if (CompressionFormat.SNAPPY.equals(compressionFormat)) {
                return new SnappyCompression<>(sparkContext, fileSystem, avroKeyClass, avroKeyClass, avroValueClass,
                        avroInputFormat, avroOutputFormat);
            } else if (CompressionFormat.LZO.equals(compressionFormat)) {
                return new LzoCompression<>(sparkContext, fileSystem, avroKeyClass, avroKeyClass, avroValueClass,
                        avroInputFormat, avroOutputFormat);
            } else if (CompressionFormat.NONE.equals(compressionFormat)) {
                return new NoneCompression<>(sparkContext, fileSystem, avroKeyClass, avroKeyClass, avroValueClass,
                        avroInputFormat, avroOutputFormat);
            } else {
                throw new IllegalArgumentException("Invalid compression format provided");
            }
        }

        public Compression<LongWritable, NullWritable, Text> forJsonFiles() {
            if (CompressionFormat.DEFLATE.equals(compressionFormat)) {
                return new DeflateCompression<>(sparkContext, fileSystem, jsonKeyClass, jsonSaveKeyClass,
                        jsonValueClass, jsonInputFormat, jsonOutputFormat);
            } else if (CompressionFormat.SNAPPY.equals(compressionFormat)) {
                return new SnappyCompression<>(sparkContext, fileSystem, jsonKeyClass, jsonSaveKeyClass, jsonValueClass,
                        jsonInputFormat, jsonOutputFormat);
            } else if (CompressionFormat.LZO.equals(compressionFormat)) {
                return new LzoCompression<>(sparkContext, fileSystem, jsonKeyClass, jsonSaveKeyClass, jsonValueClass,
                        jsonInputFormat, jsonOutputFormat);
            } else if (CompressionFormat.NONE.equals(compressionFormat)) {
                return new NoneCompression<>(sparkContext, fileSystem, jsonKeyClass, jsonSaveKeyClass, jsonValueClass,
                        jsonInputFormat, jsonOutputFormat);
            } else {
                throw new IllegalArgumentException("Invalid compression format provided");
            }
        }
    }
}
