package io.flatf.persistence.chronicle.exception;

import java.io.Serial;

public final class ChronicleAppendException extends RuntimeException {

	@Serial
	private static final long serialVersionUID = -3834475045588301175L;

	public ChronicleAppendException(Throwable cause) {
		super(cause);
	}

	public ChronicleAppendException(String message, Throwable cause) {
		super(message, cause);
	}

}
