package io.flatf.foundation.transport.zmq;

import org.zeromq.SocketType;

public class ZmqBroker extends ZmqComponent {

    ZmqBroker(ZmqConfig configurator) {
        super(configurator);
    }

    @Override
    protected SocketType getSocketType() {
        return SocketType.ROUTER;
    }

    @Override
    public ZmqType getZmqType() {
        return ZmqType.Z_BROKER;
    }

}
