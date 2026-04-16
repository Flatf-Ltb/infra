package io.flatf.transport.api;

import io.flatf.transport.exception.RequestException;

public interface Requester<T> extends Transport {

	/**
	 * 
	 * @return <T> T t
	 */
	T request() throws RequestException;

}
