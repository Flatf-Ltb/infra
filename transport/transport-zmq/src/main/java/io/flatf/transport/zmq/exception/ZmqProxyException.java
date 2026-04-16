package io.flatf.transport.zmq.exception;

import java.io.Serial;

public final class ZmqProxyException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = -5914113116108192726L;

    public ZmqProxyException(String msg, Throwable cause) {
        super(msg, cause);
    }

}
