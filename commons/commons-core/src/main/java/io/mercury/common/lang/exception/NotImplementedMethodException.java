package io.mercury.common.lang.exception;

import java.io.Serial;

public final class NotImplementedMethodException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 947213334800336824L;

    public NotImplementedMethodException(Class<?> clazz, String methodName) {
        super("ERROR [" + clazz.getName() + "] Not implemented [" + methodName + "]");
    }

}
