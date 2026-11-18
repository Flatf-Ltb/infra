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
import java.util.concurrent.locks.ReentrantLock;

import static io.flatf.common.lang.Validator.nonNull;
import static io.flatf.common.log4j2.Log4j2LoggerFactory.getLogger;

public final class ZmqPublisher<T> extends ZmqComponent implements Publisher<byte[], T>, Closeable {

    private static final Logger log = getLogger(ZmqPublisher.class);

    // default topic
    private final byte[] sendMore;

    private final BytesSerializer<T> serializer;

    // 串行化所有套接字访问: 一条消息是 sendMore + send 两帧, 多线程同时发送时帧会串到别的消息上;
    // close 也须持锁, 不能在另一线程发送到一半时关闭套接字
    private final ReentrantLock socketLock = new ReentrantLock();

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
        // HWM 须在 bind 之前设置才对之后的连接生效; 此前从未设置, 实际用的是 jeromq 默认的 1000 (2026-11-18)
        socket.setSndHWM(configurator.getHighWaterMark());
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
        // 2026-11-18: 原实现不加锁, 多线程发送时两帧会串到别的消息上 (PLAN_命令通道PUB-SUB与适配器租约 §8.3)
        // if (isRunning.get()) {
        //     byte[] bytes = serializer.serialize(msg);
        //     if (bytes != null && bytes.length > 0) {
        //         socket.sendMore(target);
        //         socket.send(bytes, ZMQ.NOBLOCK);
        //     }
        // } else
        //     log.warn("ZmqPublisher -> [{}] already closed", name);
        byte[] bytes = serializer.serialize(msg);
        if (bytes == null || bytes.length == 0)
            return;
        socketLock.lock();
        try {
            if (isRunning.get()) {
                socket.sendMore(target);
                socket.send(bytes, ZMQ.NOBLOCK);
                return;
            }
        } finally {
            socketLock.unlock();
        }
        log.warn("ZmqPublisher -> [{}] already closed", name);
    }

    @Override
    public boolean closeIgnoreException() {
        socketLock.lock();
        try {
            return super.closeIgnoreException();
        } finally {
            socketLock.unlock();
        }
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
