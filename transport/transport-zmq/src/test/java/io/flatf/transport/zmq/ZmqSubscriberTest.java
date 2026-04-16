package io.flatf.transport.zmq;

import io.flatf.transport.zmq.annotation.ZmqSubscribe;
import org.junit.Test;

import static io.flatf.transport.zmq.ZmqProtocol.IPC;

public class ZmqSubscriberTest {

    @Test
    public void test() {

    }

    @ZmqSubscribe(protocol = IPC, addr = "", ioThreads = 2)
    private void handleZmqMsg() {

    }

}
