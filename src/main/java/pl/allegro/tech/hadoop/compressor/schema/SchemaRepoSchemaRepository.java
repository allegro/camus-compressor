package pl.allegro.tech.hadoop.compressor.schema;

import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.log4j.Logger;
import org.schemarepo.client.RESTRepositoryClient;
import org.schemarepo.json.JsonUtil;

public class SchemaRepoSchemaRepository implements SchemaRepository {

    private Logger logger = Logger.getLogger(SchemaRepoSchemaRepository.class);

    private final RESTRepositoryClient client;
    private final TopicNameRetriever converter;

    public SchemaRepoSchemaRepository(String schemaRepoUrl, JsonUtil jsonUtil, TopicNameRetriever converter) {
        this.converter = converter;
        client = new RESTRepositoryClient(schemaRepoUrl, jsonUtil, false);
    }

    @Override
    public Schema findLatestSchema(String inputPath) {
        final String topic = converter.toTopicName(inputPath).replace("_avro", "");
        logger.info("Getting schema for input path " + inputPath + " and topic " + topic);
        try {
            return new Schema.Parser().parse(client.lookup(topic).latest().getSchema());
        } catch (SchemaParseException e) {
            throw new SchemaNotFoundException("Cannot parse schema", topic, e);
        }
    }
}
