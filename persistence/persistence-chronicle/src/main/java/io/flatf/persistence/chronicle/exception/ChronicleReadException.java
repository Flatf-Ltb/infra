package io.flatf.persistence.chronicle.exception;

import java.io.Serial;

public final class ChronicleReadException extends RuntimeException {

	@Serial
	private static final long serialVersionUID = -7300071826810963772L;

	public ChronicleReadException(Throwable cause) {
		super(cause);
	}

	public ChronicleReadException(String message, Throwable cause) {
		super(message, cause);
	}

}
