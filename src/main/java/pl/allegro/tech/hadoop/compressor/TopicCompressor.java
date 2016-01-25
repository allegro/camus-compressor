package pl.allegro.tech.hadoop.compressor;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class TopicCompressor {

    private FileSystem fileSystem;
    private UnitCompressor unitCompressor;
    private final TopicDateFilter topicFilter;

    private static final Logger logger = Logger.getLogger(TopicCompressor.class);

    public TopicCompressor(FileSystem fileSystem, UnitCompressor unitCompressor, TopicDateFilter topicFilter) {
        this.fileSystem = fileSystem;
        this.unitCompressor = unitCompressor;
        this.topicFilter = topicFilter;
    }

    public void compressTopic(String topicDir) throws IOException {
        logger.info(String.format("Compress topic %s", topicDir));

        compress("hourly/*/*/*/*", topicDir);
        compress("daily/*/*/*", topicDir);
    }

    public void compressTopic(Path topicDir) throws IOException {
        compressTopic(topicDir.toString());
    }

    private void compress(String unitPattern, String topicDir) throws IOException {
        String pattern = String.format("%s/%s", topicDir, unitPattern);

        final FileStatus[] fileStatuses = fileSystem.globStatus(new Path(pattern));

        for (FileStatus unitStatus : fileStatuses) {
            if (topicFilter.shouldCompressTopicDir(unitStatus.getPath().toString().replace(topicDir, ""))) {
                unitCompressor.compressUnit(unitStatus.getPath());
            }
        }
    }
}
