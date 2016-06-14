package pl.allegro.tech.hadoop.compressor.schema;

import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InputPathToTopicConverter {

    private static final Pattern pattern = Pattern.compile("/[-a-zA-Z0-9/_\\.]+/([-a-zA-Z0-9_\\.]+)/(daily|hourly)(/.*)?$");

    public String toTopicName(String inputPath) {

        final Matcher matcher = pattern.matcher(URI.create(inputPath).getPath());

        if (matcher.find()) {
            return matcher.group(1).replace("_avro", "").replace("_", ".");
        } else {
            throw new SchemaNotFoundException("Could not convert inputPath to topic", inputPath);
        }
    }
}
