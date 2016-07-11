package pl.allegro.tech.hadoop.compressor.schema;

import java.util.Map;

public class KafkaTopicNameRetriever implements TopicNameRetriever {

    private final IdentityTopicNameRetriever identityTopicNameRetriever;
    private final Map<String, String> inputPathToTopicMap;

    public KafkaTopicNameRetriever(Map<String, String> inputPathToTopicMap) {
        this.identityTopicNameRetriever = new IdentityTopicNameRetriever(inputPathToTopicMap);
        this.inputPathToTopicMap = inputPathToTopicMap;
    }

    public String toTopicName(String inputPath) {
        String onlyTopicPart = identityTopicNameRetriever.toTopicName(inputPath);
        if (!inputPathToTopicMap.containsKey(onlyTopicPart)) {
            throw new SchemaNotFoundException("Kafka topic not found", onlyTopicPart);
        }

        return inputPathToTopicMap.get(onlyTopicPart);
    }
}
