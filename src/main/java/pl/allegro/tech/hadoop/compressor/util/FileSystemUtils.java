package pl.allegro.tech.hadoop.compressor.util;

import com.hadoop.compression.lzo.LzoCodec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

public final class FileSystemUtils {

    private FileSystemUtils() { }

    public static Configuration getConfiguration(JavaSparkContext sparkContext) {
        final Configuration configuration = sparkContext.hadoopConfiguration();
        configuration.set("io.compression.codecs", LzoCodec.class.getName());
        return configuration;
    }

    public static FileSystem getFileSystem(Configuration configuration) throws IOException {
        final FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.setConf(configuration);
        return fileSystem;
    }
}
