package io.flatf.infra.transport.zmq.exception;

import java.io.Serial;

public final class ZmqBindException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = -4110788670578471831L;

    public ZmqBindException(String addr) {
        super("Unable to bind -> " + addr);
    }

}
