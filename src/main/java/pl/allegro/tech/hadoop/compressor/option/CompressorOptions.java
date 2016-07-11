package pl.allegro.tech.hadoop.compressor.option;

import org.apache.commons.lang.ClassUtils;
import org.apache.hadoop.util.ClassUtil;
import org.apache.spark.SparkConf;
import pl.allegro.tech.hadoop.compressor.schema.IdentityTopicNameRetriever;
import pl.allegro.tech.hadoop.compressor.schema.KafkaTopicNameRetriever;
import pl.allegro.tech.hadoop.compressor.schema.SchemaRepoSchemaRepository;
import pl.allegro.tech.hadoop.compressor.schema.SchemaRepository;
import pl.allegro.tech.hadoop.compressor.schema.TopicNameRetriever;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class CompressorOptions implements Serializable {

    private final Mode mode;
    private final String inputDir;
    private final int delay;
    private final CompressionFormat compression;
    private final FilesFormat format;
    private final String schemaRepositoryUrl;
    private final boolean forceSplit;
    private final String zookeeper;
    private final String workingDir;
    private final long allModeTimeout;
    private Class<? extends SchemaRepository> schemaRepositoryClass;
    private Class<? extends TopicNameRetriever> topicNameRetrieverClass;
    private final List<String> allModeExcludes;
    private final List<String> topicModePatterns;
    private boolean calculateCounts;

    @SuppressWarnings("unchecked")
    public CompressorOptions(SparkConf sparkConf) {
        mode = Mode.fromString(sparkConf.get("spark.compressor.processing.mode", "unit"));
        inputDir = sparkConf.get("spark.compressor.input.path");
        compression = CompressionFormat.fromString(sparkConf.get("spark.compressor.output.compression", "snappy"));
        delay = Integer.valueOf(sparkConf.get("spark.compressor.processing.delay", "1"));
        format = FilesFormat.fromString(sparkConf.get("spark.compressor.input.format", "json"));
        schemaRepositoryUrl = sparkConf.get("spark.compressor.avro.schema.repository.url");
        forceSplit = sparkConf.getBoolean("spark.compressor.processing.force", false);
        zookeeper = sparkConf.get("spark.compressor.zookeeper.paths", "");
        workingDir = sparkConf.get("spark.compressor.processing.working.dir", "/tmp/compressor");
        allModeExcludes = Arrays.asList(sparkConf.get("spark.compressor.processing.mode.all.excludes").split(","));
        topicModePatterns = Arrays.asList(sparkConf.get("spark.compressor.processing.mode.topic.pattern").split(","));
        allModeTimeout = sparkConf.getLong("spark.compressor.processing.mode.all.timeout.minutes", 1440L);
        calculateCounts = sparkConf.getBoolean("spark.compressor.processing.calculate.counts", true);
        try {
            schemaRepositoryClass = (Class<SchemaRepository>)ClassUtils.getClass(
                    sparkConf.get("spark.compressor.avro.schema.repository.class",
                            SchemaRepoSchemaRepository.class.getName()));
        } catch (ClassNotFoundException e) {
            schemaRepositoryClass = SchemaRepoSchemaRepository.class;
        }
        try {
            topicNameRetrieverClass = (Class<TopicNameRetriever>)ClassUtils.getClass(
                    sparkConf.get("spark.compressor.processing.topic-name-retriever.class",
                            IdentityTopicNameRetriever.class.getName()));
        } catch (ClassNotFoundException e) {
            topicNameRetrieverClass = IdentityTopicNameRetriever.class;
        }
    }

    public Mode getMode() {
        return mode;
    }

    public String getInputDir() {
        return inputDir;
    }

    public int getDelay() {
        return delay;
    }

    public CompressionFormat getCompression() {
        return compression;
    }

    public FilesFormat getFormat() {
        return format;
    }

    public String getSchemaRepositoryUrl() {
        return schemaRepositoryUrl;
    }

    public boolean isForceSplit() {
        return forceSplit;
    }

    public String getZookeeperHosts() {
        return zookeeper;
    }

    public String getWorkingDir() {
        return workingDir;
    }

    public long getAllModeTimeout() {
        return allModeTimeout;
    }

    public Class<? extends SchemaRepository> getSchemaRepositoryClass() {
        return schemaRepositoryClass;
    }

    public Class<? extends TopicNameRetriever> getTopicNameRetrieverClass() {
        return topicNameRetrieverClass;
    }

    public List<String> getAllModeExcludes() {
        return allModeExcludes;
    }

    public List<String> getTopicModePatterns() {
        return topicModePatterns;
    }

    public boolean isCalculateCounts() {
        return calculateCounts;
    }

    @Override
    public String toString() {
        return "CompressorOptions{" +
                "mode=" + mode +
                ", inputDir='" + inputDir + '\'' +
                ", delay=" + delay +
                ", compression=" + compression +
                ", format=" + format +
                ", schemaRepositoryUrl=" + schemaRepositoryUrl +
                ", zookeeper=" + zookeeper +
                ", forceSplit=" + forceSplit +
                '}';
    }

    public enum Mode {
        ALL, TOPIC, UNIT;

        static Mode fromString(String mode) {
            return Mode.valueOf(mode.toUpperCase());
        }
    }
}
