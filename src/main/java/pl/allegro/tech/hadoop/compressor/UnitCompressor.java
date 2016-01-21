package pl.allegro.tech.hadoop.compressor;

import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import pl.allegro.tech.hadoop.compressor.compression.Compression;

public class UnitCompressor {

    private static final Logger logger = Logger.getLogger(UnitCompressor.class);
    private static final int BYTES_IN_KB = 1024;
    private final FileSystem fileSystem;
    private final JavaSparkContext context;
    private final Compression compression;
    private final InputAnalyser inputAnalyser;

    public UnitCompressor(JavaSparkContext context, FileSystem fileSystem, Compression compression, InputAnalyser inputAnalyser) {
        this.context = context;
        this.fileSystem = fileSystem;
        this.compression = compression;
        this.inputAnalyser = inputAnalyser;
    }

    public void compressUnit(Path unitPath) throws IOException {
        compressUnit(unitPath.toString());
    }

    public void compressUnit(String unitPath) throws IOException {
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

            final JavaRDD<String> rdd = context.textFile(inputPath).repartition(inputAnalyser.countInputSplits(inputPath));
            context.setJobGroup("compression", String.format("%s (%s)", unitPath, FileUtils.byteCountToDisplaySize(inputSize)));
            compression.compress(rdd, outputDir);

            cleanup(unitPath, outputDir);
        } else {
            logger.info(String.format("Found 0 files in dir %s", unitPath));
        }
    }

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

