package io.flatf.configuration.nacos;

import java.io.Serial;

public class NacosConnectionException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 7392466235739011687L;

    public NacosConnectionException(String msg, Throwable e) {
        super(msg, e);
    }

}
