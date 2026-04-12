package io.flatf.transport.api;

import io.flatf.common.annotation.thread.AsyncFunction;
import io.flatf.transport.exception.ConnectionBreakException;
import io.flatf.transport.exception.ReceiverStartException;

public interface Receiver extends Transport, Runnable {

    /**
     * Start receive
     *
     * @throws ReceiverStartException e
     */
    @AsyncFunction
    void receive() throws ReceiverStartException;

    /**
     * Reconnect
     *
     * @throws ConnectionBreakException e0
     * @throws ReceiverStartException   e1
     */
    void reconnect() throws ConnectionBreakException, ReceiverStartException;

    @Override
    default void run() {
        receive();
    }

}
