package io.flatf.infra.persistence.rocksdb.exception;

import java.io.Serial;

public final class RocksRuntimeException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = -6940482392634006619L;

    public RocksRuntimeException(String msg) {
        super(msg);
    }

    public RocksRuntimeException(Throwable throwable) {
        super(throwable);
    }

    public RocksRuntimeException(String msg, Throwable throwable) {
        super(msg, throwable);
    }

}
