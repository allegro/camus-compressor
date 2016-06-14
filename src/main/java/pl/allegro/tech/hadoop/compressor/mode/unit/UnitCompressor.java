package pl.allegro.tech.hadoop.compressor.mode.unit;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import pl.allegro.tech.hadoop.compressor.mode.Compress;
import pl.allegro.tech.hadoop.compressor.util.InputAnalyser;

import java.io.IOException;

public abstract class UnitCompressor implements Compress {

    private static final Logger logger = Logger.getLogger(UnitCompressor.class);
    private static final int BYTES_IN_KB = 1024;
    protected final FileSystem fileSystem;

    protected final InputAnalyser inputAnalyser;

    public UnitCompressor(FileSystem fileSystem, InputAnalyser inputAnalyser) {
        this.fileSystem = fileSystem;
        this.inputAnalyser = inputAnalyser;
    }

    public void compress(Path unitPath) throws IOException {
        compress(unitPath.toString());
    }

    public void compress(String unitPath) throws IOException {
        final String inputPath = String.format("%s/*", unitPath);
        final String outputDir = getTemporaryDirPath(unitPath);

        if (inputAnalyser.shouldCompress(inputPath)) {
            if (fileExists(outputDir)) {
                if (fileExists(getSuccessFilePath(outputDir))) {
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
            repartition(inputPath, outputDir, jobGroup, inputAnalyser.countInputSplits(inputPath));

            cleanup(unitPath, outputDir);
        } else {
            logger.info(String.format("Found 0 files in dir %s", unitPath));
        }

    }

    protected abstract void repartition(String inputPath, String outputDir, String jobGroup, int inputSplits)
            throws IOException;

    private String getTemporaryDirPath(String hourPath) {
        return String.format("/tmp/compressor/%s", hourPath.replace(":", "").replace('/', '-'));
    }

    private String getSuccessFilePath(String outputDir) {
        return String.format("%s/_SUCCESS", outputDir);
    }

    private void cleanup(String inputDir, String outputDir) throws IOException {
        logger.info(String.format("Cleaning input dir %s and success file %s", inputDir, getSuccessFilePath(outputDir)));
        remove(inputDir, true);
        remove(getSuccessFilePath(outputDir), false);
        move(outputDir, inputDir);
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
