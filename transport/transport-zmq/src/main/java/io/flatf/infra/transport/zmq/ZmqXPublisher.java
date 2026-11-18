package io.flatf.infra.transport.zmq;

import io.flatf.common.thread.Sleep;
import io.flatf.infra.serialization.specific.BytesSerializer;
import io.flatf.infra.transport.api.Publisher;
import io.flatf.infra.transport.exception.PublishFailedException;
import io.flatf.infra.transport.zmq.exception.ZmqBindException;
import org.slf4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.io.Closeable;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static io.flatf.common.lang.Validator.nonNull;
import static io.flatf.common.log4j2.Log4j2LoggerFactory.getLogger;

/**
 * 能看到订阅变化的发布端（{@code XPUB}，主动 bind），与普通 {@link ZmqSubscriber} 配对。
 *
 * <p>发送与 {@link ZmqPublisher} 相同：两帧（主题帧 + 主体帧），按订阅前缀过滤，积压满时静默丢弃，
 * 不开 {@code XPUB_NODROP}（有旁听方的通道上，一个慢订阅者会让所有发送失败）。
 *
 * <p>多出的是订阅事件：订阅方的订阅生效时回调 {@code onSubscription(true, topic)}，退订或断开时回调
 * {@code onSubscription(false, topic)}。用法是"收到某主题的订阅事件后再向它发送"，这样避开慢加入：
 * 事件报告时订阅已经进了过滤表，之后发出的消息不会因订阅尚未生效被丢。
 *
 * <h2>事件语义（XPUB 默认，未开 VERBOSE）</h2>
 * 同一主题只在<b>第一个</b>订阅者到来时报告订阅、<b>最后一个</b>离开时报告退订；中间的重复订阅不报告。
 * 因此事件回答的是"这个主题现在有没有订阅者"，数不出有几个。订阅还在也不代表对端进程还活着
 * （进程卡住但连接未断时订阅一直保留），存活要另靠心跳判断。
 *
 * <h2>线程模型</h2>
 * 同 {@link ZmqAsyncServer}：一把锁串行化全部套接字访问；订阅事件在自有轮询线程上读取并回调，
 * {@link #start()} 之前到达的订阅留在套接字里，启动后照常报告；发送可来自任意线程。
 */
@ThreadSafe
public final class ZmqXPublisher<T> extends ZmqComponent implements Publisher<byte[], T>, Closeable {

    private static final Logger log = getLogger(ZmqXPublisher.class);

    private static final long RECEIVE_IDLE_SLEEP_NANOS = 1_000_000L;

    private static final long CLOSE_JOIN_TIMEOUT_MILLIS = 2_000L;

    // XPUB 收到的订阅消息: 首字节 1 为订阅, 0 为退订, 其余为主题
    private static final byte SUBSCRIBE = 1;

    private static final byte UNSUBSCRIBE = 0;

    /**
     * 订阅变化回调，在发布端的轮询线程上执行。
     */
    @FunctionalInterface
    public interface SubscriptionListener {

        /**
         * @param subscribed true 为该主题有了第一个订阅者, false 为最后一个订阅者离开
         * @param topic      订阅的主题前缀, 空数组表示订阅全部
         */
        void onSubscription(boolean subscribed, @Nonnull byte[] topic);
    }

    // default topic
    private final byte[] sendMore;

    private final BytesSerializer<T> serializer;

    private final SubscriptionListener listener;

    private final ReentrantLock socketLock = new ReentrantLock();

    private final AtomicBoolean pollStarted = new AtomicBoolean(false);

    private volatile Thread pollThread;

    /**
     * @param configurator ZmqConfig
     * @param topic        默认主题
     * @param serializer   BytesSerializer<T>
     * @param listener     订阅变化回调, 在本类的轮询线程上执行
     */
    ZmqXPublisher(@Nonnull ZmqConfig configurator,
                  @Nonnull String topic,
                  @Nonnull BytesSerializer<T> serializer,
                  @Nonnull SubscriptionListener listener) {
        super(configurator);
        nonNull(topic, "topic");
        nonNull(serializer, "serializer");
        nonNull(listener, "listener");
        this.sendMore = topic.getBytes(ZMQ.CHARSET);
        this.serializer = serializer;
        this.listener = listener;
        socket.setReceiveTimeOut(0);
        socket.setSndHWM(configurator.getHighWaterMark());
        socket.setRcvHWM(configurator.getHighWaterMark());
        socket.setLinger(0);
        context.setLinger(0);
        var addr = configurator.getAddr().fullUri();
        if (socket.bind(addr))
            log.info("ZmqXPublisher bound addr -> {}", addr);
        else {
            log.error("ZmqXPublisher unable to bind -> {}", addr);
            throw new ZmqBindException(addr);
        }
        setTcpKeepAlive(configurator.getTcpKeepAlive());
        this.name = "ZXPub$" + addr + ":" + topic;
        newStartTime();
    }

    @Override
    protected SocketType getSocketType() {
        return SocketType.XPUB;
    }

    @Override
    public ZmqType getZmqType() {
        return ZmqType.Z_XPUBLISHER;
    }

    /**
     * 启动订阅事件轮询线程。重复调用无效果。
     */
    public void start() {
        if (!pollStarted.compareAndSet(false, true)) {
            log.warn("ZmqXPublisher -> [{}] poll thread already started", name);
            return;
        }
        this.pollThread = Thread.ofPlatform().name(name + "-subscription-poller").start(this::pollLoop);
    }

    @Override
    public void publish(@Nonnull T msg) {
        publish(sendMore, msg);
    }

    @Override
    public void publish(@Nonnull byte[] target, @Nonnull T msg) throws PublishFailedException {
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
        log.warn("ZmqXPublisher -> [{}] already closed", name);
    }

    private void pollLoop() {
        log.info("ZmqXPublisher -> [{}] subscription poller started", name);
        while (isRunning.get()) {
            byte[] frame = receiveOnce();
            if (frame == null) {
                Sleep.parkNanos(RECEIVE_IDLE_SLEEP_NANOS);
                continue;
            }
            if (frame.length == 0 || (frame[0] != SUBSCRIBE && frame[0] != UNSUBSCRIBE)) {
                log.warn("ZMQ_XPUBLISHER SUBSCRIPTION_IGNORED name={} reason=UNKNOWN_FORMAT length={}",
                        name, frame.length);
                continue;
            }
            boolean subscribed = frame[0] == SUBSCRIBE;
            byte[] topic = Arrays.copyOfRange(frame, 1, frame.length);
            log.info("ZMQ_XPUBLISHER SUBSCRIPTION_CHANGED name={} subscribed={} topic={}",
                    name, subscribed, new String(topic, ZMQ.CHARSET));
            try {
                listener.onSubscription(subscribed, topic);
            } catch (RuntimeException e) {
                // 回调抛出不能杀死轮询线程, 否则之后的订阅变化全部静默丢失
                log.error("ZMQ_XPUBLISHER LISTENER_FAILED name={} subscribed={} topic={}",
                        name, subscribed, new String(topic, ZMQ.CHARSET), e);
            }
        }
        log.info("ZmqXPublisher -> [{}] subscription poller stopped", name);
    }

    private byte[] receiveOnce() {
        socketLock.lock();
        try {
            if (!isRunning.get())
                return null;
            byte[] frame = socket.recv(ZMQ.NOBLOCK);
            // 订阅消息是单帧; 多出的帧丢弃, 否则会串到下一条订阅消息上
            while (frame != null && socket.hasReceiveMore())
                socket.recv();
            return frame;
        } catch (ZMQException e) {
            if (isRunning.get())
                throw e;
            return null;
        } finally {
            socketLock.unlock();
        }
    }

    /**
     * 先让轮询线程停下并汇合，再关闭套接字，理由同 {@link ZmqAsyncClient#closeIgnoreException()}。
     */
    @Override
    public boolean closeIgnoreException() {
        if (!isRunning.compareAndSet(true, false)) {
            log.warn("ZmqXPublisher -> [{}] already closed, cannot be called again", name);
            return context.isClosed();
        }
        Thread thread = this.pollThread;
        if (thread != null) {
            try {
                thread.join(CLOSE_JOIN_TIMEOUT_MILLIS);
                if (thread.isAlive())
                    log.warn("ZmqXPublisher -> [{}] subscription poller did not stop within {} ms",
                            name, CLOSE_JOIN_TIMEOUT_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("ZmqXPublisher -> [{}] interrupted while joining subscription poller", name);
            }
        }
        socketLock.lock();
        try {
            socket.close();
            context.close();
            newEndTime();
            log.info("ZmqXPublisher -> [{}] closed, running duration millis -> {}", name, getRunningDuration());
        } finally {
            socketLock.unlock();
        }
        return context.isClosed();
    }

}
