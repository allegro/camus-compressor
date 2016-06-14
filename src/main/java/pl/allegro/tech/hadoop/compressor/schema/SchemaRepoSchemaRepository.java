package pl.allegro.tech.hadoop.compressor.schema;

import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.schemarepo.client.RESTRepositoryClient;
import org.schemarepo.json.JsonUtil;

public class SchemaRepoSchemaRepository implements SchemaRepository {

    private final RESTRepositoryClient client;
    private final InputPathToTopicConverter converter;

    public SchemaRepoSchemaRepository(String schemaRepoUrl, JsonUtil jsonUtil, InputPathToTopicConverter converter) {
        this.converter = converter;
        client = new RESTRepositoryClient(schemaRepoUrl, jsonUtil, false);
    }

    @Override
    public Schema findLatestSchema(String inputPath) {
        final String topic = converter.toTopicName(inputPath);
        try {
            return new Schema.Parser().parse(client.lookup(topic).latest().getSchema());
        } catch (SchemaParseException e) {
            throw new SchemaNotFoundException("Cannot parse schema", topic, e);
        }
    }
}
