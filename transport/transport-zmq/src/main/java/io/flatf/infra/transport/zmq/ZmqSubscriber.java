package io.flatf.infra.transport.zmq;

import io.flatf.infra.transport.api.Subscriber;
import io.flatf.infra.transport.attr.Topics;
import io.flatf.infra.transport.zmq.exception.ZmqConnectionException;
import org.slf4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Arrays;
import java.util.function.BiConsumer;

import static io.flatf.common.lang.Validator.nonNull;
import static io.flatf.common.log4j2.Log4j2LoggerFactory.getLogger;

/**
 * @author yellow013
 */
public final class ZmqSubscriber extends ZmqComponent implements Subscriber {

    private static final Logger log = getLogger(ZmqSubscriber.class);

    // topics
    private final Topics topics;

    // 订阅消息消费者
    private final BiConsumer<byte[], byte[]> consumer;

    ZmqSubscriber(@Nonnull ZmqConfig configurator,
                  @Nonnull Topics topics,
                  @Nonnull BiConsumer<byte[], byte[]> consumer)
            throws ZmqConnectionException {
        super(configurator);
        nonNull(topics, "topics");
        nonNull(consumer, "consumer");
        this.topics = topics;
        this.consumer = consumer;
        var addr = configurator.getAddr().fullUri();
        if (socket.connect(addr)) {
            log.info("ZmqSubscriber connected addr -> {}", addr);
        } else {
            log.error("ZmqSubscriber unable to connect addr -> {}", addr);
            throw new ZmqConnectionException(addr);
        }
//        setTcpKeepAlive(configurator.getTcpKeepAlive() == null
//                // 如果TcpKeepAlive为空, 使用默认TcpKeepAlive配置
//                ? TcpKeepAlive.enable().setCount(10).setIdle(30).setInterval(30)
//                : configurator.getTcpKeepAlive());
        setTcpKeepAlive(configurator.getTcpKeepAlive());
        // 订阅主题
        topics.each(topic -> socket.subscribe(topic.getBytes(ZMQ.CHARSET)));
        this.name = "ZSub$" + addr + "/" + topics;
        newStartTime();
    }

    public Topics getTopics() {
        return topics;
    }

    @Override
    protected SocketType getSocketType() {
        return SocketType.SUB;
    }

    @Override
    public ZmqType getZmqType() {
        return ZmqType.Z_SUBSCRIBER;
    }

    @Override
    public void subscribe() {
        while (isRunning.get()) {
            byte[] topic = socket.recv();
            log.debug("received topic, length: {}", topic.length);
            byte[] msg = socket.recv();
            log.debug("received msg, length: {}", topic.length);
            consumer.accept(topic, msg);
        }
        log.info("ZmqSubscriber -> [{}] already closed", name);
    }

    @Override
    public void reconnect() {
        throw new UnsupportedOperationException("ZmqSubscriber unsupported reconnect");
    }

    @Override
    public void run() {
        subscribe();
    }

    public static void main(String[] args) {
        try (ZmqSubscriber subscriber = ZmqConfig.tcp("127.0.0.1", 13001).ioThreads(2).createSubscriber(
                Topics.with("test"),
                (topic, msg) -> System.out.println(Arrays.toString(topic) + " -> " + Arrays.toString(msg)))) {
            subscriber.subscribe();
        } catch (IOException ignored) {
        }
    }

}
