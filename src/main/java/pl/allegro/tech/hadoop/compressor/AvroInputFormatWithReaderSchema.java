package pl.allegro.tech.hadoop.compressor;

import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class AvroInputFormatWithReaderSchema<T> extends AvroInputFormat<T> {

    @Override
    public RecordReader<AvroWrapper<T>, NullWritable>
    getRecordReader(InputSplit split, JobConf job, Reporter reporter)
            throws IOException {
        reporter.setStatus(split.toString());
        return new AvroRecordReaderWithSchemaReader<T>(job, (FileSplit)split);
    }
}
