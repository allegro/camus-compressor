package pl.allegro.tech.hadoop.compressor;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class TopicCompressor {

    private static final String DATE_FORMAT = "yyyy/MM/dd";
    private FileSystem fileSystem;
    private UnitCompressor unitCompressor;

    private static final Logger logger = Logger.getLogger(TopicCompressor.class);

    public TopicCompressor(FileSystem fileSystem, UnitCompressor unitCompressor) {
        this.fileSystem = fileSystem;
        this.unitCompressor = unitCompressor;
    }

    public void compressTopic(String topicDir) throws IOException {
        logger.info(String.format("Compress topic %s", topicDir));

        compress("hourly/*/*/*/*", topicDir);
        compress("daily/*/*/*", topicDir);
    }

    private void compress(String unitPattern, String topicDir) throws IOException {
        String pattern = String.format("%s/%s", topicDir, unitPattern);
        final FileStatus[] fileStatuses = fileSystem.globStatus(new Path(pattern));
        for (FileStatus unitStatus : fileStatuses) {
            if (dirShouldNotBeCompressed(unitStatus, topicDir)) {
                continue;
            }
            unitCompressor.compressUnit(unitStatus.getPath());
        }
    }

    private boolean dirShouldNotBeCompressed(FileStatus unitStatus, String topicDir) {
        return unitStatus.getPath().toString().replace(topicDir, "").contains(todayDate());
    }

    private static String todayDate() {
        final SimpleDateFormat formatter = new SimpleDateFormat(DATE_FORMAT);
        return formatter.format(new Date());
    }

    public void compressTopic(Path topicDir) throws IOException {
        compressTopic(topicDir.toString());
    }
}
