package uk.sparcit.espload.exception;

public class ESPLoadException extends RuntimeException {

    public ESPLoadException(String message) {
        super(message);
    }

    public ESPLoadException(Exception exc) {
        super(exc);
    }

    public ESPLoadException(String message, Exception exc) {
        super(message, exc);
    }
}