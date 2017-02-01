package pl.allegro.tech.hadoop.compressor.schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.gson.Gson;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;

import javax.ws.rs.core.MediaType;

public class SchemaRegistrySchemaRepository implements SchemaRepository {

    private final String registryUrl;
    private final Gson gson;
    private final TopicNameRetriever converter;
    private final Client client;

    public SchemaRegistrySchemaRepository(String registryUrl, Gson gson, TopicNameRetriever converter) {
        this.registryUrl = registryUrl;
        this.gson = gson;
        this.converter = converter;
        client = getClient();
    }

    @Override
    public Schema findLatestSchema(String inputPath) {
        final String topic = converter.toTopicName(inputPath).replace("_avro", "");

        try {
            final String schemaEntryString = client.resource(registryUrl + "/subjects/" + topic + "/versions/latest")
                    .accept(MediaType.APPLICATION_JSON_TYPE)
                    .get(String.class);
            final SchemaEntry schema = gson.fromJson(schemaEntryString, SchemaEntry.class);
            return new Schema.Parser().parse(schema.getSchema());
        } catch (SchemaParseException | UniformInterfaceException e) {
            throw new SchemaNotFoundException("Cannot parse schema", topic, e);
        }
    }

    private synchronized Client getClient() {
        DefaultClientConfig clientConfig = new DefaultClientConfig();
        clientConfig.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

        return Client.create(clientConfig);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SchemaEntry {

        private String schema;

        public SchemaEntry() {
        }

        public String getSchema() {
            return schema;
        }

        public void setSchema(String schema) {
            this.schema = schema;
        }
    }
}
