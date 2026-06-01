package io.flatf.foundation.persistence.chronicle.exception;

import io.flatf.foundation.common.character.Separator;

import java.io.Serial;

public final class ChronicleIOException extends RuntimeException {

	@Serial
	private static final long serialVersionUID = 439989412345254532L;

	public ChronicleIOException(Throwable cause) {
		super(cause);
	}

	public ChronicleIOException(String message, Throwable cause) {
		super(message + Separator.LINE_SEPARATOR + "because : " + cause.getMessage(), cause);
	}

}
