package pl.allegro.tech.hadoop.compressor;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroRecordReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

public class AvroRecordReaderWithSchemaReader<T> extends AvroRecordReader<T> {

    public AvroRecordReaderWithSchemaReader(JobConf job, FileSplit split)
            throws IOException {
        super(DataFileReader.openReader
                        (new FsInput(split.getPath(), job),
                                AvroJob.createInputDataModel(job)
                                        .createDatumReader(AvroJob.getInputSchema(job), AvroJob.getOutputSchema(job))),
                split);
    }
}
