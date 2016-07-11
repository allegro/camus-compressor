package pl.allegro.tech.hadoop.compressor.schema;

import org.apache.avro.Schema;

public interface SchemaRepository {

    Schema findLatestSchema(String topic);
}
