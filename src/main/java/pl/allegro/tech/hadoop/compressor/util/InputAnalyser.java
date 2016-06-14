package pl.allegro.tech.hadoop.compressor.util;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import pl.allegro.tech.hadoop.compressor.compression.Compression;

import java.io.IOException;

public class InputAnalyser {
    private final FileSystem fileSystem;
    private final Compression compression;
    private final boolean forceSplit;

    public InputAnalyser(FileSystem fileSystem, Compression compression, boolean forceSplit) {
        this.fileSystem = fileSystem;
        this.compression = compression;
        this.forceSplit = forceSplit;
    }

    public boolean shouldCompress(String inputPath) throws IOException {
        return forceSplit || countNonCompressedInputSize(inputPath) != 0L;
    }

    public long countInputSize(String inputPath) throws IOException {
        long total = 0;
        for (FileStatus file : fileSystem.globStatus(new Path(inputPath))) {
            total += file.getLen();
        }
        return total;
    }

    public int countInputSplits(String inputPath) throws IOException {
        return compression.getSplits(countInputSize(inputPath));
    }

    private long countNonCompressedInputSize(String inputPattern) throws IOException {
        long total = 0;
        for (FileStatus file : fileSystem.globStatus(new Path(inputPattern))) {
            if (!file.getPath().toString().endsWith("." + compression.getExtension())) {
                total += file.getLen();
            }
        }
        return total;
    }
}
