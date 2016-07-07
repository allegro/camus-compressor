package pl.allegro.tech.hadoop.compressor.schema;

import java.net.URI;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Use when topics' folders on HDFS have identical to topics' names.
 */
public class IdentityTopicNameRetriever implements TopicNameRetriever {

    private static final Pattern pattern = Pattern.compile("/[-a-zA-Z0-9/_\\.]+/([-a-zA-Z0-9_\\.]+)/(daily|hourly)(/.*)?$");

    public IdentityTopicNameRetriever(Map<String, String> topicMap) {
    }

    public String toTopicName(String inputPath) {

        final Matcher matcher = pattern.matcher(URI.create(inputPath).getPath());

        if (matcher.find()) {
            return matcher.group(1);
        } else {
            throw new SchemaNotFoundException("Could not convert inputPath to topic", inputPath);
        }
    }
}
