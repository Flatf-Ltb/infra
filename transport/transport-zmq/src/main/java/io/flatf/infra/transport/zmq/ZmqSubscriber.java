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

    // 接收超时: subscribe() 借此定期检查是否已请求停止, 不会无限阻塞在 recv 上
    private static final int RECEIVE_TIMEOUT_MILLIS = 500;

    // 单帧消息 (主题写在帧内) 交给消费者时使用的主题
    private static final byte[] EMPTY_TOPIC = new byte[0];

    // topics
    private final Topics topics;

    // 订阅消息消费者
    private final BiConsumer<byte[], byte[]> consumer;

    // 正在执行 subscribe() 的线程; close 时等它退出后再关闭套接字, 因为 ZMQ 套接字不能跨线程并发访问
    private volatile Thread loopThread;

    private volatile boolean stopRequested;

    ZmqSubscriber(@Nonnull ZmqConfig configurator,
                  @Nonnull Topics topics,
                  @Nonnull BiConsumer<byte[], byte[]> consumer)
            throws ZmqConnectionException {
        super(configurator);
        nonNull(topics, "topics");
        nonNull(consumer, "consumer");
        this.topics = topics;
        this.consumer = consumer;
        // HWM 须在 connect 之前设置; 此前从未设置, 实际用的是 jeromq 默认的 1000 (2026-11-18)
        socket.setRcvHWM(configurator.getHighWaterMark());
        socket.setReceiveTimeOut(RECEIVE_TIMEOUT_MILLIS);
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

    /**
     * 阻塞接收直到 {@link #close()}。两帧消息以 (主题帧, 主体帧) 交给消费者; 单帧消息 (主题写在帧内) 以
     * (空数组, 整帧) 交给消费者; 超过两帧的消息不符合本类的约定, 丢弃并记 warn。
     */
    @Override
    public void subscribe() {
        // 2026-11-18: 原实现无接收超时、假定每条消息固定两帧, 单帧消息会被两两配对, 关闭时以 ETERM 异常退出
        // (PLAN_命令通道PUB-SUB与适配器租约 §8.3)
        // while (isRunning.get()) {
        //     byte[] topic = socket.recv();
        //     log.debug("received topic, length: {}", topic.length);
        //     byte[] msg = socket.recv();
        //     log.debug("received msg, length: {}", topic.length);
        //     consumer.accept(topic, msg);
        // }
        // log.info("ZmqSubscriber -> [{}] already closed", name);
        loopThread = Thread.currentThread();
        try {
            while (isRunning.get() && !stopRequested) {
                byte[] first = socket.recv();
                if (first == null)
                    continue;
                if (!socket.hasReceiveMore()) {
                    consumer.accept(EMPTY_TOPIC, first);
                    continue;
                }
                byte[] second = socket.recv();
                if (socket.hasReceiveMore()) {
                    int extra = 0;
                    while (socket.hasReceiveMore()) {
                        socket.recv();
                        extra++;
                    }
                    log.warn("ZMQ_SUBSCRIBER MESSAGE_DROPPED name={} reason=TOO_MANY_FRAMES frames={}",
                            name, 2 + extra);
                    continue;
                }
                if (second != null)
                    consumer.accept(first, second);
            }
        } finally {
            loopThread = null;
        }
        log.info("ZmqSubscriber -> [{}] already closed", name);
    }

    @Override
    public boolean closeIgnoreException() {
        stopRequested = true;
        Thread loop = loopThread;
        if (loop != null && loop != Thread.currentThread()) {
            try {
                loop.join(RECEIVE_TIMEOUT_MILLIS * 4L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (loop.isAlive())
                log.warn("ZMQ_SUBSCRIBER LOOP_STILL_RUNNING name={} thread={}", name, loop.getName());
        }
        return super.closeIgnoreException();
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
