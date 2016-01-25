package pl.allegro.tech.hadoop.compressor;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import pl.allegro.tech.hadoop.compressor.compression.Compression;
import pl.allegro.tech.hadoop.compressor.compression.LzoCompression;
import pl.allegro.tech.hadoop.compressor.compression.NoneCompression;
import pl.allegro.tech.hadoop.compressor.compression.SnappyCompression;
import pl.allegro.tech.hadoop.compressor.util.FileSystemUtils;

public final class Compressor {

    private Compressor() { }

    public static void main(String[] args) throws IOException {
        final String compressObject = args[0];
        final String inputDir = args[1];
        final String compressor = args[2];
        final String delay = args[3];

        final SparkConf sparkConf = new SparkConf()
                .setAppName(Compressor.class.getName());
        final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        final Configuration configuration = FileSystemUtils.getConfiguration(sparkContext);
        final FileSystem fileSystem = FileSystemUtils.getFileSystem(configuration);

        final Compression compression = getCompression(compressor, fileSystem, sparkContext);

        final InputAnalyser inputAnalyser = createInputAnalyser(args, fileSystem, compression);
        final UnitCompressor unitCompressor = new UnitCompressor(sparkContext, fileSystem, compression, inputAnalyser);

        final TopicDateFilter topicFilter = new TopicDateFilter(Integer.valueOf(delay));
        final TopicCompressor topicCompressor = new TopicCompressor(fileSystem, unitCompressor, topicFilter);
        final CamusCompressor camusCompressor = new CamusCompressor(fileSystem, topicCompressor, Integer.valueOf(sparkConf.get("spark.executor.instances")));

        if ("all".equals(compressObject)) {
            camusCompressor.compressAll(inputDir);
        } else if ("topic".equals(compressObject)) {
            topicCompressor.compressTopic(inputDir);
        } else if ("unit".equals(compressObject)) {
            unitCompressor.compressUnit(inputDir);
        }
    }

    private static Compression getCompression(String compressor,
            FileSystem fileSystem, JavaSparkContext sparkContext) {
        if ("none".equals(compressor)) {
            return new NoneCompression(fileSystem, sparkContext);
        } else if ("lzo".equals(compressor)) {
            return new LzoCompression(sparkContext, fileSystem);
        }
        return new SnappyCompression(fileSystem, sparkContext);
    }

    private static InputAnalyser createInputAnalyser(String[] args, FileSystem fileSystem, Compression compression) {
        boolean forceSplit = false;
        if (Arrays.asList(args).contains("--force")) {
            forceSplit = true;
        }
        return new InputAnalyser(fileSystem, compression, forceSplit);
    }
}
