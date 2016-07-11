package pl.allegro.tech.hadoop.compressor.option;

public enum FilesFormat {
    AVRO, JSON;

    public static FilesFormat fromString(String format) {
        return FilesFormat.valueOf(format.toUpperCase());
    }
}
