package pl.allegro.tech.hadoop.compressor.schema;

public class SchemaNotFoundException extends RuntimeException {

    private static final long serialVersionUID = -5873618512938340074L;

    private final String topic;

    public SchemaNotFoundException(String message, String topic, Exception cause) {
        super(message, cause);
        this.topic = topic;
    }

    public SchemaNotFoundException(String s, String topic) {
        super(s);
        this.topic = topic;
    }

    @Override
    public String toString() {
        return super.toString() + " -- topic: " + topic;
    }

    public String getTopic() {
        return topic;
    }
}
