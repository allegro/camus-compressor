package pl.allegro.tech.hadoop.compressor.exception;

public class InvalidCountsException extends RuntimeException {

    private final long beforeCount;
    private final long afterCount;

    public InvalidCountsException(String message, long beforeCount, long afterCount) {
        super(message);
        this.beforeCount = beforeCount;
        this.afterCount = afterCount;
    }

    @Override
    public String toString() {
        return "InvalidCountsException{" +
                "message=" + getMessage() +
                ", beforeCount=" + beforeCount +
                ", afterCount=" + afterCount +
                '}';
    }
}
