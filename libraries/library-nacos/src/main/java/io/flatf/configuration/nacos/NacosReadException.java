package io.flatf.configuration.nacos;

import java.io.Serial;

public final class NacosReadException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = -4866898838811364365L;

    public NacosReadException(String msg) {
        super(msg);
    }

    public NacosReadException(Throwable e) {
        super(e.getMessage(), e);
    }

    public NacosReadException(String msg, Throwable e) {
        super(msg, e);
    }

}
