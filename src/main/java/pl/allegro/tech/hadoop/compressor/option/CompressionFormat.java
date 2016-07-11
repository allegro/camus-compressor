package pl.allegro.tech.hadoop.compressor.option;

public enum CompressionFormat {
    SNAPPY, DEFLATE, LZO, NONE;

    public static CompressionFormat fromString(String format) {
        return CompressionFormat.valueOf(format.toUpperCase());
    }
}
