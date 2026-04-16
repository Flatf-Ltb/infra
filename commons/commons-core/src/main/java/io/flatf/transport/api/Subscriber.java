package io.flatf.transport.api;

import io.flatf.common.annotation.thread.AsyncFunction;

public interface Subscriber extends Transport, Runnable {

    /**
     * Start to subscribe
     */
    @AsyncFunction
    void subscribe();

    /**
     * Reconnect
     */
    void reconnect();

}
