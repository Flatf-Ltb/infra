package io.flatf.transport.zmq.exception;

import java.io.Serial;

public final class ZmqConnectionException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = -6368403482174464676L;

    public ZmqConnectionException(String addr) {
        super("Unable to connect -> " + addr);
    }

}
