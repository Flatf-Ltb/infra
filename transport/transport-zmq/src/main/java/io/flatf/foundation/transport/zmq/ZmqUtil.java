package io.flatf.foundation.transport.zmq;

import org.zeromq.ZMQ;

public final class ZmqUtil {

    private ZmqUtil() {
    }

    public static byte[] toTopicWithBytes(String topic) {
        if (topic == null)
            return "".getBytes(ZMQ.CHARSET);
        return topic.getBytes(ZMQ.CHARSET);
    }
    
}
