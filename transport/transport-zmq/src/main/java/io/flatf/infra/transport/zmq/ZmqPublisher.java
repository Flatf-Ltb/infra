package io.flatf.infra.transport.zmq;

import io.flatf.infra.serialization.specific.BytesSerializer;
import io.flatf.common.thread.Sleep;
import io.flatf.infra.transport.api.Publisher;
import io.flatf.infra.transport.exception.PublishFailedException;
import io.flatf.infra.transport.zmq.exception.ZmqBindException;
import org.slf4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.util.Random;

import static io.flatf.common.lang.Validator.nonNull;
import static io.flatf.common.log4j2.Log4j2LoggerFactory.getLogger;

public final class ZmqPublisher<T> extends ZmqComponent implements Publisher<byte[], T>, Closeable {

    private static final Logger log = getLogger(ZmqPublisher.class);

    // default topic
    private final byte[] sendMore;

    private final BytesSerializer<T> serializer;

    /**
     * @param configurator ZmqConfigurator
     * @param topic        String
     * @param serializer   BytesSerializer<T>
     */
    ZmqPublisher(@Nonnull ZmqConfig configurator,
                 @Nonnull String topic,
                 @Nonnull BytesSerializer<T> serializer) {
        super(configurator);
        nonNull(topic, "topic");
        nonNull(serializer, "serializer");
        this.sendMore = topic.getBytes(ZMQ.CHARSET);
        this.serializer = serializer;
        var addr = configurator.getAddr().fullUri();
        if (socket.bind(addr))
            log.info("ZmqPublisher bound addr -> {}", addr);
        else {
            log.error("ZmqPublisher unable to bind -> {}", addr);
            throw new ZmqBindException(addr);
        }
        setTcpKeepAlive(configurator.getTcpKeepAlive());
        this.name = "ZPub$" + addr + ":" + topic;
        newStartTime();
    }

    public BytesSerializer<T> getSerializer() {
        return serializer;
    }

    @Override
    protected SocketType getSocketType() {
        return SocketType.PUB;
    }

    @Override
    public ZmqType getZmqType() {
        return ZmqType.Z_PUBLISHER;
    }

    @Override
    public void publish(@Nonnull T msg) {
        publish(sendMore, msg);
    }

    @Override
    public void publish(@Nonnull byte[] target, @Nonnull T msg) throws PublishFailedException {
        if (isRunning.get()) {
            byte[] bytes = serializer.serialize(msg);
            if (bytes != null && bytes.length > 0) {
                socket.sendMore(target);
                socket.send(bytes, ZMQ.NOBLOCK);
            }
        } else
            log.warn("ZmqPublisher -> [{}] already closed", name);
    }

    public static void main(String[] args) throws Exception {
        try (ZmqPublisher<String> publisher = ZmqConfig.tcp("127.0.0.1", 13001)
                .ioThreads(2)
                .createPublisherWithString("test")) {
            Random random = new Random();
            while (true) {
                publisher.publish(String.valueOf(random.nextInt()));
                Sleep.millis(1000);
            }
        }
    }

}
