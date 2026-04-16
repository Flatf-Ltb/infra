package io.flatf.serialization.json;

import java.io.Serial;

/**
 * @author yellow013
 */
public final class JsonParseException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 9000408863460789219L;

    public JsonParseException(String json, Throwable cause) {
        super("Parsing JSON -> " + json + " , Throw exception -> [" + cause.getClass().getName() + "]", cause);
    }

    public JsonParseException(Throwable cause) {
        super("Parsing JSON throw exception -> [ " + cause.getClass().getName() + "]", cause);
    }

    public JsonParseException(String message) {
        super(message);
    }

}
