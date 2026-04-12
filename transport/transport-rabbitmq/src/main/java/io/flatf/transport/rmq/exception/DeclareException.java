package io.flatf.transport.rmq.exception;

import com.rabbitmq.client.BuiltinExchangeType;

import java.io.Serial;
import java.util.Map;

public final class DeclareException extends Exception {

    @Serial
    private static final long serialVersionUID = -7352101640279556300L;

    /**
     * @param cause Throwable
     */
    private DeclareException(Throwable cause) {
        super(cause);
    }

    /**
     * @param message String
     * @param cause   Throwable
     */
    private DeclareException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * @param queue      String
     * @param durable    boolean
     * @param exclusive  boolean
     * @param autoDelete boolean
     * @param args       Map<String, Object>
     * @param cause      Throwable
     * @return DeclareException
     */
    public static DeclareException declareQueueError(String queue, boolean durable,
                                                     boolean exclusive, boolean autoDelete,
                                                     Map<String, Object> args, Throwable cause) {
        return new DeclareException("Declare queue error -> queue==[" + queue + "], durable==[" + durable
                + "], exclusive==[" + exclusive + "], autoDelete==[" + autoDelete + "], args==[" + args + "]", cause);
    }

    /**
     * @param exchange   String
     * @param type       BuiltinExchangeType
     * @param durable    boolean
     * @param autoDelete boolean
     * @param internal   boolean
     * @param args       Map<String, Object>
     * @param cause      Throwable
     * @return DeclareException
     */
    public static DeclareException declareExchangeError(String exchange, BuiltinExchangeType type,
                                                        boolean durable, boolean autoDelete, boolean internal,
                                                        Map<String, Object> args, Throwable cause) {
        return new DeclareException(
                "Declare exchange error -> exchange==[" + exchange + "], type==[" + type + "], durable==[" + durable
                        + "], autoDelete==[" + autoDelete + "], internal==[" + internal + "], args==[" + args + "]",
                cause);
    }

    /**
     * @param queue      String
     * @param exchange   String
     * @param routingKey String
     * @param cause      Throwable
     * @return DeclareException
     */
    public static DeclareException bindQueueError(String queue, String exchange,
                                                  String routingKey, Throwable cause) {
        return new DeclareException("Bind queue error -> queue==[" + queue + "], exchange==[" + exchange
                + "], routingKey==[" + routingKey + "]", cause);
    }

    /**
     * @param destExchange   String
     * @param sourceExchange String
     * @param routingKey     String
     * @param cause          Throwable
     * @return DeclareException
     */
    public static DeclareException bindExchangeError(String destExchange, String sourceExchange, String routingKey,
                                                     Throwable cause) {
        return new DeclareException("Bind exchange error -> destExchange==[" + destExchange + "], sourceExchange==["
                + sourceExchange + "], routingKey==[" + routingKey + "]", cause);
    }

    /**
     * @param cause Throwable
     * @return DeclareException
     */
    public static DeclareException becauseOf(Throwable cause) {
        return new DeclareException(cause);
    }

}
