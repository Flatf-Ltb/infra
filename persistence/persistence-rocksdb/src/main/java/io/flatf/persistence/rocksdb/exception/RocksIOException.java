package io.flatf.persistence.rocksdb.exception;

import java.io.IOException;
import java.io.Serial;

public final class RocksIOException extends IOException {

    @Serial
    private static final long serialVersionUID = -4712312419639419833L;

    public RocksIOException(String msg, IOException ioe) {
        super(msg, ioe);
    }

}
