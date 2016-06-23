package pl.allegro.tech.hadoop.compressor.schema;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class KafkaTopicNameRetrieverTest {

    private final String inputPath;
    private final String expectedTopic;

    private HashMap<String, String> topicMap = new HashMap<>();

    private KafkaTopicNameRetriever converter = new KafkaTopicNameRetriever(
            topicMap);

    public KafkaTopicNameRetrieverTest(String inputPath, String expectedTopic) {
        this.inputPath = inputPath;
        this.expectedTopic = expectedTopic;
    }

    @Before
    public void setUp() throws Exception {
        topicMap.put("topic_name", "topic.name");
        topicMap.put("topic_name_avro", "topic.name_avro");
    }

    @Parameters
    public static Collection<Object[]> dataProvider() {
        return Arrays.asList(new Object[][] {
                { "/input/path/topic_name/daily/2016/03/04/part1.avro",                 "topic.name" },
                { "/input/path/topic_name/hourly/2016/03/04/04/part1.avro",             "topic.name" },
                { "/input/path/topic_name/daily/*/*/*/*.avro",                          "topic.name" },
                { "/input/path/topic_name/hourly/*/*/*/*.avro",                         "topic.name" },
                { "/input/path/topic_name/daily/*/*/*/*",                               "topic.name" },
                { "/input/path/topic_name/hourly/*/*/*/*/*",                            "topic.name" },
                { "hdfs://zeus/user/piotr.wikiel/topic_name_avro/daily/2015/11/25/*",   "topic.name_avro" }
        });
    }

    @Test
    public void shouldConvertInputPathToTopicName() throws Exception {
        assertEquals(expectedTopic, converter.toTopicName(inputPath));
    }

    @Test(expected = SchemaNotFoundException.class)
    public void shouldThrowExceptionWhenCannotConvertInputPathToTopicName() throws Exception {
        converter.toTopicName("/abc");
    }
}