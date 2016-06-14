package pl.allegro.tech.hadoop.compressor.schema;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class InputPathToTopicConverterTest {

    private final String inputPath;
    private final String expectedTopic;

    private InputPathToTopicConverter converter = new InputPathToTopicConverter();

    public InputPathToTopicConverterTest(String inputPath, String expectedTopic) {
        this.inputPath = inputPath;
        this.expectedTopic = expectedTopic;
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
                { "hdfs://zeus/user/piotr.wikiel/topic_name_avro/daily/2015/11/25/*",   "topic.name" }
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