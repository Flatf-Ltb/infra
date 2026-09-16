package io.flatf.infra.transport.zmq;

public enum ZmqType {

    Z_BROKER,

    Z_PROXY,

    Z_PUBLISHER,

    Z_SUBSCRIBER,

    Z_SENDER,

    Z_RECEIVER,

    /**
     * DEALER，异步请求端。见 {@link ZmqAsyncClient}。
     */
    Z_ASYNC_CLIENT,

    /**
     * ROUTER，异步应答端。见 {@link ZmqAsyncServer}。
     */
    Z_ASYNC_SERVER,

}
