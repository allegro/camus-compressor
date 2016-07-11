package pl.allegro.tech.hadoop.compressor.schema;

public interface TopicNameRetriever {
    String toTopicName(String inputPath);
}
