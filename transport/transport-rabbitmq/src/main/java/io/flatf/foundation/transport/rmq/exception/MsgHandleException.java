package io.flatf.foundation.transport.rmq.exception;

import java.io.Serial;

public class MsgHandleException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = -8006184062312891950L;

    /**
     * @param message String
     * @param cause   Throwable
     */
    public MsgHandleException(String message, Throwable cause) {
        super(message, cause);
    }

}
