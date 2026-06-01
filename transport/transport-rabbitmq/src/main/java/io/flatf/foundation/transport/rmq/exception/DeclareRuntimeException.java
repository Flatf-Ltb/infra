package io.flatf.foundation.transport.rmq.exception;

import java.io.Serial;

public final class DeclareRuntimeException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = -3951956735581216086L;

    /**
     * @param cause DeclareException
     */
    public DeclareRuntimeException(DeclareException cause) {
        super(cause);
    }

}
