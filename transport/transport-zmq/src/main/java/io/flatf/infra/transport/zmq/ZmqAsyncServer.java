package io.flatf.infra.transport.zmq;

import io.flatf.common.thread.Sleep;
import io.flatf.infra.transport.zmq.exception.ZmqBindException;
import org.slf4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static io.flatf.common.lang.Validator.nonNull;
import static io.flatf.common.log4j2.Log4j2LoggerFactory.getLogger;

/**
 * 异步应答端（{@code ROUTER}，主动 bind），与 {@link ZmqAsyncClient} 配对。
 *
 * <p>取代 {@code SUB} 作为命令接收端的理由见 {@link ZmqAsyncClient} 的类注释。这里只补一点：
 * {@code ROUTER} 会为每个连上来的对端维护身份，应答能准确回到发起方，因此一条通道上可以同时服务
 * 多个请求端，而 {@code PUB/SUB} 只能广播 —— 在 PUB/SUB 下每个订阅端都会收到<b>全部</b>命令，
 * 靠接收端自觉过滤，漏过滤就是跨账户误执行。
 *
 * <p><b>方向约定：服务方 bind，请求方 connect。</b>执行命令的一方是服务方，所以由它监听。
 *
 * <h2>应答时机由业务决定</h2>
 * {@link Exchange#reply(byte[])} 可以在处理回调里同步调用，也可以在业务处理完成后从<b>其他线程</b>调用。
 * 这是有意的：若强制"处理函数返回值即应答"，一次慢处理会顶住整条接收通道。
 * 未调用 {@code reply} 的请求就是没有应答 —— 请求方会按自己的超时策略发现它。
 *
 * <h2>线程模型</h2>
 * 同 {@link ZmqAsyncClient}：一把锁串行化全部套接字访问并提供内存屏障；接收在自有轮询线程上，
 * {@code reply} 可来自任意线程。
 */
@ThreadSafe
public final class ZmqAsyncServer extends ZmqComponent implements Closeable {

    private static final Logger log = getLogger(ZmqAsyncServer.class);

    private static final int SEND_TIMEOUT_MILLIS = 50;

    private static final long RECEIVE_IDLE_SLEEP_NANOS = 1_000_000L;

    private static final long CLOSE_JOIN_TIMEOUT_MILLIS = 2_000L;

    private final Consumer<Exchange> handler;

    private final ReentrantLock socketLock = new ReentrantLock();

    private final AtomicBoolean pollStarted = new AtomicBoolean(false);

    private volatile Thread pollThread;

    /**
     * 一次请求及其应答通道。
     */
    public interface Exchange {

        /**
         * 请求负载（单帧）。
         */
        byte[] payload();

        /**
         * 发起方身份，由 {@code ROUTER} 分配或对端自行设置，可用于日志定位。
         */
        String senderId();

        /**
         * 向发起方回送应答，可从任意线程调用。
         *
         * @return 是否已交给传输层；false 表示积压已满或本端已关闭，应答<b>没有</b>发出
         */
        boolean reply(@Nonnull byte[] reply);

        /**
         * 以 UTF-8 回送字符串应答。
         */
        default boolean reply(@Nonnull String reply) {
            return reply(reply.getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * @param configurator ZmqConfig
     * @param handler      请求处理回调，在本类的轮询线程上执行
     */
    ZmqAsyncServer(@Nonnull ZmqConfig configurator,
                   @Nonnull Consumer<Exchange> handler) {
        super(configurator);
        nonNull(handler, "handler");
        this.handler = handler;
        socket.setSendTimeOut(SEND_TIMEOUT_MILLIS);
        socket.setReceiveTimeOut(0);
        socket.setSndHWM(configurator.getHighWaterMark());
        socket.setRcvHWM(configurator.getHighWaterMark());
        socket.setLinger(0);
        // ZContext 自身也有 linger，且默认不为 0：未投递的消息会让 close() 阻塞到超时。
        // 实测该场景下关闭一个无对端的 DEALER 要挂住几十秒，必须显式清零。
        context.setLinger(0);
        var addr = configurator.getAddr().fullUri();
        if (socket.bind(addr))
            log.info("ZmqAsyncServer bound addr -> {}", addr);
        else {
            log.error("ZmqAsyncServer unable to bind -> {}", addr);
            throw new ZmqBindException(addr);
        }
        setTcpKeepAlive(configurator.getTcpKeepAlive());
        this.name = "ZAsyncServer$" + addr;
        newStartTime();
    }

    @Override
    protected SocketType getSocketType() {
        return SocketType.ROUTER;
    }

    @Override
    public ZmqType getZmqType() {
        return ZmqType.Z_ASYNC_SERVER;
    }

    /**
     * 启动请求轮询线程。重复调用无效果。
     */
    public void start() {
        if (!pollStarted.compareAndSet(false, true)) {
            log.warn("ZmqAsyncServer -> [{}] poll thread already started", name);
            return;
        }
        this.pollThread = Thread.ofPlatform().name(name + "-request-poller").start(this::pollLoop);
    }

    private void pollLoop() {
        log.info("ZmqAsyncServer -> [{}] request poller started", name);
        while (isRunning.get()) {
            RouterFrames frames = receiveOnce();
            if (frames == null) {
                Sleep.parkNanos(RECEIVE_IDLE_SLEEP_NANOS);
                continue;
            }
            try {
                handler.accept(new RouterExchange(frames.identity(), frames.payload()));
            } catch (RuntimeException e) {
                // 回调抛出不能杀死轮询线程，否则后续请求全部静默丢失
                log.error("ZmqAsyncServer -> [{}] request handler threw, senderId={}, payloadLength={}",
                        name, toSenderId(frames.identity()), frames.payload().length, e);
            }
        }
        log.info("ZmqAsyncServer -> [{}] request poller stopped", name);
    }

    /**
     * {@code ROUTER} 收到的是 {@code [identity][payload]} 两帧。身份帧由 ZMQ 附加，必须原样带回，
     * 否则应答无法路由。
     */
    @Nullable
    private RouterFrames receiveOnce() {
        socketLock.lock();
        try {
            if (!isRunning.get())
                return null;
            byte[] identity = socket.recv(ZMQ.NOBLOCK);
            if (identity == null)
                return null;
            if (!socket.hasReceiveMore()) {
                log.warn("ZmqAsyncServer -> [{}] discarded identity-only message, senderId={}",
                        name, toSenderId(identity));
                return null;
            }
            byte[] payload = socket.recv();
            // 协议约定负载是单帧；多出来的帧只能丢弃，否则会串到下一条消息上
            while (socket.hasReceiveMore()) {
                socket.recv();
                log.warn("ZmqAsyncServer -> [{}] discarded unexpected extra frame, senderId={}",
                        name, toSenderId(identity));
            }
            return new RouterFrames(identity, payload);
        } catch (ZMQException e) {
            if (isRunning.get())
                throw e;
            return null;
        } finally {
            socketLock.unlock();
        }
    }

    private boolean sendReply(byte[] identity, byte[] reply) {
        if (!isRunning.get())
            return false;
        socketLock.lock();
        try {
            if (!isRunning.get())
                return false;
            if (!socket.sendMore(identity)) {
                log.warn("ZmqAsyncServer -> [{}] reply identity frame not sent, senderId={}",
                        name, toSenderId(identity));
                return false;
            }
            if (socket.send(reply, 0))
                return true;
            // 身份帧已出、负载帧未出：套接字停在半条消息上，无法从外部回退。
            // 记为错误而不是警告 —— 这条通道后续的应答都可能错位。
            log.error("ZmqAsyncServer -> [{}] reply payload frame not sent after identity frame, senderId={}",
                    name, toSenderId(identity));
            return false;
        } catch (ZMQException e) {
            if (isRunning.get())
                throw e;
            return false;
        } finally {
            socketLock.unlock();
        }
    }

    private static String toSenderId(byte[] identity) {
        return new String(identity, ZMQ.CHARSET);
    }

    private record RouterFrames(byte[] identity, byte[] payload) {
    }

    private final class RouterExchange implements Exchange {

        private final byte[] identity;
        private final byte[] payload;

        private RouterExchange(byte[] identity, byte[] payload) {
            this.identity = identity;
            this.payload = payload;
        }

        @Override
        public byte[] payload() {
            return payload;
        }

        @Override
        public String senderId() {
            return toSenderId(identity);
        }

        @Override
        public boolean reply(@Nonnull byte[] reply) {
            nonNull(reply, "reply");
            return sendReply(identity, reply);
        }
    }

    /**
     * 先让轮询线程停下并汇合，再关闭套接字，理由同 {@link ZmqAsyncClient#closeIgnoreException()}。
     */
    @Override
    public boolean closeIgnoreException() {
        if (!isRunning.compareAndSet(true, false)) {
            log.warn("ZmqAsyncServer -> [{}] already closed, cannot be called again", name);
            return context.isClosed();
        }
        Thread thread = this.pollThread;
        if (thread != null) {
            try {
                thread.join(CLOSE_JOIN_TIMEOUT_MILLIS);
                if (thread.isAlive())
                    log.warn("ZmqAsyncServer -> [{}] request poller did not stop within {} ms",
                            name, CLOSE_JOIN_TIMEOUT_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("ZmqAsyncServer -> [{}] interrupted while joining request poller", name);
            }
        }
        socketLock.lock();
        try {
            socket.close();
            context.close();
            newEndTime();
            log.info("ZmqAsyncServer -> [{}] closed, running duration millis -> {}", name, getRunningDuration());
        } finally {
            socketLock.unlock();
        }
        return context.isClosed();
    }

}
