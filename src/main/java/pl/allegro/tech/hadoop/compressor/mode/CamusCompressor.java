package pl.allegro.tech.hadoop.compressor.mode;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CamusCompressor implements Compress {

    private static final int COMPRESSOR_TIMEOUT_DAYS = 10;
    private FileSystem fileSystem;
    private TopicCompressor topicCompressor;
    static final List<String> EXCLUDES = Collections.unmodifiableList(Arrays.asList("base", "history", "tables"));

    private Logger logger = Logger.getLogger(CamusCompressor.class);
    private ExecutorService executor;

    public CamusCompressor(FileSystem fileSystem, TopicCompressor topicCompressor, int numOfExecutors) {
        this.fileSystem = fileSystem;
        this.topicCompressor = topicCompressor;
        executor = Executors.newFixedThreadPool(numOfExecutors);
    }

    public List<Path> getTopicDirs(Path camusDir) throws IOException {
        List<Path> paths = new ArrayList<>();
        final FileStatus[] fileStatuses = fileSystem.listStatus(camusDir);
        for (FileStatus fileStatus : fileStatuses) {
            if (EXCLUDES.contains(fileStatus.getPath().getName())) {
                continue;
            }
            paths.add(fileStatus.getPath());
        }
        return paths;
    }

    public void compress(Path camusDir) throws IOException {
        logger.info(String.format("Compress all %s", camusDir));
        final List<Path> topicDirs = getTopicDirs(camusDir);
        for (final Path topicDir : topicDirs) {
            executor.submit(compressTopic(topicDir));
        }
        executor.shutdown();
        try {
            executor.awaitTermination(COMPRESSOR_TIMEOUT_DAYS, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            logger.error("Executor interrupted");
        }

    }

    private Runnable compressTopic(final Path topicDir) {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    topicCompressor.compress(topicDir);
                } catch (IOException e) {
                    logger.error("Exception occured on compressing " + topicDir, e);
                }
            }
        };
    }

    public void compress(String camusDir) throws IOException {
        compress(new Path(camusDir));
    }
}
