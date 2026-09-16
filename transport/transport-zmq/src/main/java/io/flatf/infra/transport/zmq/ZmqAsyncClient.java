package io.flatf.infra.transport.zmq;

import io.flatf.common.thread.Sleep;
import io.flatf.infra.transport.zmq.exception.ZmqConnectionException;
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
 * 异步请求端（{@code DEALER}，主动 connect），与 {@link ZmqAsyncServer} 配对。
 *
 * <p><b>为什么不用 PUB/SUB。</b>PUB 在订阅端未连接时**静默丢弃**消息，且 {@code publish} 没有返回值 ——
 * 发送方无从知道消息是否发出去了。对行情这类可丢的流没问题，对"下单""撤单"这类命令是致命的：
 * 丢一条意味着报单从未发出，而调用方会一直等一个永远不会到来的回报。
 *
 * <p><b>为什么不用 REQ/REP（{@link ZmqSender} / {@link ZmqReceiver}）。</b>REQ 强制 send-recv 严格轮转，
 * {@code send()} 会**阻塞调用线程**直到对端应答；对端一旦失联，调用线程永久卡死，且超时后的 REQ 套接字
 * 处于不可恢复状态，必须销毁重建。命令通常从事件流水线的消费线程上发出，在那里阻塞等于停掉整条流水线。
 *
 * <p><b>DEALER 的取舍。</b>发送不等待应答，调用线程立刻拿到"是否已交给传输层"的结论；
 * 应答异步到达，由调用方按业务自身的请求 ID 关联。本类<b>不做</b>请求-应答配对与超时管理 ——
 * 那是业务语义，属于调用方。
 *
 * <h2>消息格式</h2>
 * 请求与应答都是<b>单帧</b>。业务关联 ID 应当放在负载内部（大多数协议本来就有 requestId），
 * 不额外占用一个帧：多帧发送在中途失败会让套接字停在"半条消息"的状态，而 ZMQ 没有可靠的回退手段。
 * 对端身份由 {@code ROUTER} 自行附加与剥离，本端不可见。
 *
 * <h2>线程模型</h2>
 * ZMQ 套接字不是线程安全的。本类用一把 {@link java.util.concurrent.locks.ReentrantLock 锁}串行化
 * 全部套接字访问，锁同时提供跨线程所需的内存屏障：
 * <ul>
 *   <li>{@link #send(byte[])} 在<b>调用方线程</b>上直接发送，不入队、不切换线程 —— 命令路径上一次
 *       线程切换的代价（唤醒 + 调度延迟）比发送本身大得多；</li>
 *   <li>应答由 {@link #start()} 启动的轮询线程接收，空闲时短暂休眠。</li>
 * </ul>
 */
@ThreadSafe
public final class ZmqAsyncClient extends ZmqComponent implements Closeable {

    private static final Logger log = getLogger(ZmqAsyncClient.class);

    /**
     * 发送超时。到达发送高水位（对端不可达且积压已满）时，{@code send} 最多阻塞这么久便返回
     * {@link SendResult#BACKPRESSURE}，而不是无限期挂住调用线程。
     */
    private static final int SEND_TIMEOUT_MILLIS = 50;

    /**
     * 应答轮询线程在无消息时的休眠间隔。应答是确认帧，毫秒级延迟无关紧要；
     * 这里换来的是接近零的空转开销。
     */
    private static final long REPLY_IDLE_SLEEP_NANOS = 1_000_000L;

    /**
     * 关闭时等待轮询线程退出的上限。
     */
    private static final long CLOSE_JOIN_TIMEOUT_MILLIS = 2_000L;

    private final Consumer<byte[]> replyConsumer;

    /**
     * 串行化全部套接字访问。jeromq 的套接字允许跨线程使用，前提是访问之间存在完整的内存屏障 ——
     * 锁的获取/释放正好提供该屏障。
     */
    private final ReentrantLock socketLock = new ReentrantLock();

    private final AtomicBoolean pollStarted = new AtomicBoolean(false);

    private volatile Thread pollThread;

    /**
     * 发送结论。调用方<b>必须</b>读取 —— 不读返回值的发送等于没有可靠性。
     */
    public enum SendResult {

        /**
         * 已交给传输层。<b>不表示对端已收到</b>：确认要靠对端的应答。
         */
        SENT,

        /**
         * 传输层积压已满（对端不可达或消费过慢）。消息<b>没有</b>发出。
         */
        BACKPRESSURE,

        /**
         * 本端已关闭。消息没有发出。
         */
        CLOSED;

        public boolean isSent() {
            return this == SENT;
        }
    }

    /**
     * @param configurator  ZmqConfig
     * @param identity      套接字身份，null 或空则由 ZMQ 自动分配。显式设置后对端日志里能看出是谁连上来的
     * @param replyConsumer 应答回调，在本类的轮询线程上执行
     */
    ZmqAsyncClient(@Nonnull ZmqConfig configurator,
                   @Nullable String identity,
                   @Nonnull Consumer<byte[]> replyConsumer) {
        super(configurator);
        nonNull(replyConsumer, "replyConsumer");
        this.replyConsumer = replyConsumer;
        if (identity != null && !identity.isBlank())
            socket.setIdentity(identity.getBytes(ZMQ.CHARSET));
        socket.setSendTimeOut(SEND_TIMEOUT_MILLIS);
        // 非阻塞收：轮询线程不能持锁阻塞，否则发送方会被应答轮询挡住
        socket.setReceiveTimeOut(0);
        socket.setSndHWM(configurator.getHighWaterMark());
        socket.setRcvHWM(configurator.getHighWaterMark());
        // 关闭时不为未发出的消息滞留，否则 close() 会挂住
        socket.setLinger(0);
        // ZContext 自身也有 linger，且默认不为 0：未投递的消息会让 close() 阻塞到超时。
        // 实测该场景下关闭一个无对端的 DEALER 要挂住几十秒，必须显式清零。
        context.setLinger(0);
        var addr = configurator.getAddr().fullUri();
        if (socket.connect(addr))
            log.info("ZmqAsyncClient connected addr -> {}, identity -> {}", addr, identity);
        else {
            log.error("ZmqAsyncClient unable to connect addr -> {}", addr);
            throw new ZmqConnectionException(addr);
        }
        setTcpKeepAlive(configurator.getTcpKeepAlive());
        this.name = "ZAsyncClient$" + addr;
        newStartTime();
    }

    @Override
    protected SocketType getSocketType() {
        return SocketType.DEALER;
    }

    @Override
    public ZmqType getZmqType() {
        return ZmqType.Z_ASYNC_CLIENT;
    }

    /**
     * 启动应答轮询线程。重复调用无效果。
     */
    public void start() {
        if (!pollStarted.compareAndSet(false, true)) {
            log.warn("ZmqAsyncClient -> [{}] poll thread already started", name);
            return;
        }
        this.pollThread = Thread.ofPlatform().name(name + "-reply-poller").start(this::pollLoop);
    }

    /**
     * 发送一条请求，<b>不等待</b>应答。可从任意线程调用。
     *
     * @param payload 单帧负载，业务关联 ID 应在其内部
     * @return 发送结论，调用方必须读取
     */
    public SendResult send(@Nonnull byte[] payload) {
        nonNull(payload, "payload");
        if (!isRunning.get())
            return SendResult.CLOSED;
        socketLock.lock();
        try {
            if (!isRunning.get())
                return SendResult.CLOSED;
            return socket.send(payload, 0) ? SendResult.SENT : SendResult.BACKPRESSURE;
        } catch (ZMQException e) {
            if (isRunning.get())
                throw e;
            // 关闭过程中上下文被终止，属于正常退出路径
            return SendResult.CLOSED;
        } finally {
            socketLock.unlock();
        }
    }

    /**
     * 以 UTF-8 发送字符串负载。
     */
    public SendResult send(@Nonnull String payload) {
        nonNull(payload, "payload");
        return send(payload.getBytes(StandardCharsets.UTF_8));
    }

    private void pollLoop() {
        log.info("ZmqAsyncClient -> [{}] reply poller started", name);
        while (isRunning.get()) {
            byte[] reply = receiveOnce();
            if (reply == null) {
                Sleep.parkNanos(REPLY_IDLE_SLEEP_NANOS);
                continue;
            }
            try {
                replyConsumer.accept(reply);
            } catch (RuntimeException e) {
                // 回调抛出不能杀死轮询线程，否则后续应答全部静默丢失
                log.error("ZmqAsyncClient -> [{}] reply consumer threw, replyLength={}", name, reply.length, e);
            }
        }
        log.info("ZmqAsyncClient -> [{}] reply poller stopped", name);
    }

    @Nullable
    private byte[] receiveOnce() {
        socketLock.lock();
        try {
            if (!isRunning.get())
                return null;
            return socket.recv(ZMQ.NOBLOCK);
        } catch (ZMQException e) {
            if (isRunning.get())
                throw e;
            return null;
        } finally {
            socketLock.unlock();
        }
    }

    /**
     * 先让轮询线程停下并汇合，再关闭套接字。
     *
     * <p>顺序是有意的：若先关套接字，轮询线程可能正停在 {@code recv} 上，会撞出 {@link ZMQException}
     * 或访问已释放的原生资源。
     */
    @Override
    public boolean closeIgnoreException() {
        if (!isRunning.compareAndSet(true, false)) {
            log.warn("ZmqAsyncClient -> [{}] already closed, cannot be called again", name);
            return context.isClosed();
        }
        Thread thread = this.pollThread;
        if (thread != null) {
            try {
                thread.join(CLOSE_JOIN_TIMEOUT_MILLIS);
                if (thread.isAlive())
                    log.warn("ZmqAsyncClient -> [{}] reply poller did not stop within {} ms",
                            name, CLOSE_JOIN_TIMEOUT_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("ZmqAsyncClient -> [{}] interrupted while joining reply poller", name);
            }
        }
        socketLock.lock();
        try {
            socket.close();
            context.close();
            newEndTime();
            log.info("ZmqAsyncClient -> [{}] closed, running duration millis -> {}", name, getRunningDuration());
        } finally {
            socketLock.unlock();
        }
        return context.isClosed();
    }

}
