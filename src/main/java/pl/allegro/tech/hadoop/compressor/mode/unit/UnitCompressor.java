package pl.allegro.tech.hadoop.compressor.mode.unit;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.storage.StorageLevel;
import pl.allegro.tech.hadoop.compressor.exception.InvalidCountsException;
import pl.allegro.tech.hadoop.compressor.mode.Compress;
import pl.allegro.tech.hadoop.compressor.util.InputAnalyser;

import java.io.IOException;

public abstract class UnitCompressor<K, V> implements Compress {

    private static final Logger logger = Logger.getLogger(UnitCompressor.class);
    private static final int BYTES_IN_KB = 1024;
    protected final FileSystem fileSystem;

    protected final InputAnalyser inputAnalyser;
    private final String workingPath;
    private final String backupDir;
    private final boolean calcCounts;

    public UnitCompressor(FileSystem fileSystem, InputAnalyser inputAnalyser, String workingPath, String backupDir, boolean calcCounts) {
        this.fileSystem = fileSystem;
        this.inputAnalyser = inputAnalyser;
        this.workingPath = workingPath;
        this.backupDir = backupDir;
        this.calcCounts = calcCounts;
    }

    public void compress(Path unitPath) throws IOException {
        compress(unitPath.toString());
    }

    public void compress(String unitPath) throws IOException {
        final String inputPath = String.format("%s/*", unitPath);
        final String outputDir = getTemporaryDirPath(unitPath);

        if (inputAnalyser.shouldCompress(inputPath)) {
            if (fileExists(outputDir)) {
                if (fileExists(getSuccessFilePath(outputDir)) && !fileExists(getInvalidCountFilePath(outputDir))) {
                    logger.info(String.format("Directory %s already compressed, removing input", outputDir));
                    cleanup(unitPath, outputDir);
                    return;
                } else {
                    logger.info(String.format("Compressing %s has not finished last time, retrying", outputDir));
                    remove(outputDir, true);
                }
            }
            long inputSize = inputAnalyser.countInputSize(inputPath);
            logger.info(String.format("Compress unit %s to %s (%d KB)", unitPath, outputDir, inputSize / BYTES_IN_KB));
            String jobGroup = String.format("%s (%s)", unitPath, FileUtils.byteCountToDisplaySize(inputSize));

            JobConf inputPathJobConf = createJobConf(inputPath, inputPath);
            JavaPairRDD<K, V> fetchedRddForInputPath = fetchRDD(inputPathJobConf);
            fetchedRddForInputPath.persist(StorageLevel.MEMORY_AND_DISK());
            long beforeCount = countIfRequested(fetchedRddForInputPath);
            repartition(fetchedRddForInputPath, outputDir, jobGroup, inputPathJobConf, inputAnalyser.countInputSplits(inputPath));
            JobConf outputPathJobConf = createJobConf(outputDir, inputPath);
            JavaPairRDD<K, V> fetchedRddForOutputPath = fetchRDD(outputPathJobConf);
            long afterCount = countIfRequested(fetchedRddForOutputPath);
            fetchedRddForInputPath.unpersist();

            if (beforeCount != afterCount) {
                logger.error(String.format("Counts are different: %d vs. %d", beforeCount, afterCount));
                createInvalidCountFilePath(outputDir);
                throw new InvalidCountsException("Counts are different before and after compression.",
                        beforeCount, afterCount);
            }

            cleanup(unitPath, outputDir);
        } else {
            logger.info(String.format("Found 0 files in dir %s", unitPath));
        }

    }

    private long countIfRequested(JavaPairRDD<K, V> rdd) throws IOException {
        if (calcCounts) {
            return countOutputDir(rdd);
        } else {
            return -1L;
        }
    }

    protected abstract JobConf createJobConf(String path, String schemaReaderPath);

    protected abstract JavaPairRDD<K, V> fetchRDD(JobConf jobConf) throws IOException;

    protected abstract long countOutputDir(JavaPairRDD<K, V> rdd) throws IOException;

    protected abstract void repartition(JavaPairRDD<K, V> rdd, String outputPath, String jobGroup, JobConf jobConf, int inputSplits)
            throws IOException;

    private String getTemporaryDirPath(String hourPath) {
        return String.format(workingPath + "/%s", hourPath.replace(":", "").replace('/', '-'));
    }

    private String getSuccessFilePath(String outputDir) {
        return String.format("%s/_SUCCESS", outputDir);
    }

    private void cleanup(String inputDir, String outputDir) throws IOException {
        logger.info(String.format("Cleaning input dir %s and success file %s", inputDir, getSuccessFilePath(outputDir)));
        String pathToStoreBackup = getBackupPath(inputDir);
        logger.info(String.format("path to store backup %s", pathToStoreBackup));
        createBackupDir(pathToStoreBackup);
        if (move(inputDir, pathToStoreBackup)) {
            if (move(outputDir, inputDir)) {
                if (!remove(pathToStoreBackup, true)) {
                    logger.info(String.format("Could not delete data from backup folder %s", pathToStoreBackup));
                }
                if (!remove(getSuccessFilePath(inputDir), false)) {
                    logger.info(String.format("Could not delete success compaction file from dir %s", getSuccessFilePath(inputDir)));
                }
            } else {
                throw new RuntimeException(String.format("Could not move compacted data from %s to origin folder %s", outputDir , inputDir));
            }
        } else {
            throw new RuntimeException(String.format("Could not move original data from %s to backup folder %s", inputDir , pathToStoreBackup));
        }
    }

    private String getBackupPath(String inputDir) {
        Path inputPath = new Path(inputDir);
        return backupDir + inputPath.toUri().getPath();
    }

    private boolean createBackupDir(String backupPath) throws IOException {
        return fileSystem.mkdirs(new Path(backupPath));
    }

    private boolean createInvalidCountFilePath(String outputDir) throws IOException {
        return fileSystem.createNewFile(new Path(getInvalidCountFilePath(outputDir)));
    }

    private String getInvalidCountFilePath(String outputDir) {
        return String.format("%s/_COUNT_INVALID", outputDir);
    }

    private boolean move(String outputDir, String inputDir) throws IOException {
        return fileSystem.rename(new Path(outputDir), new Path(inputDir));
    }

    private boolean fileExists(String outputDir) throws IOException {
        return fileSystem.exists(new Path(outputDir));
    }

    public boolean remove(String path, boolean recursive) throws IOException {
        return fileSystem.delete(new Path(path), recursive);
    }
}
