package io.flatf.transport.aeron;

import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import io.flatf.common.serialization.specific.BytesSerializer;
import io.flatf.transport.TransportCfg;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static io.flatf.common.lang.Validator.nonEmpty;
import static io.flatf.common.lang.Validator.nonNull;
import static io.flatf.common.log4j2.Log4j2LoggerFactory.getLogger;

/**
 * Aeron 配置与工厂, 对应 ZMQ 中的 {@code ZmqCfg}.
 *
 * <p>每个 {@link AeronPublisher} / {@link AeronSubscriber} 持有独立的 {@link Aeron} 客户端,
 * 通过 {@link #newAeron()} 创建; 嵌入式 {@link MediaDriver} 在 JVM 内仅启动一次 (懒加载单例).
 */
public final class AeronCfg implements TransportCfg {

    private static final Logger log = getLogger(AeronCfg.class);

    private static final AtomicReference<MediaDriver> SHARED_DRIVER = new AtomicReference<>();

    /**
     * 进程间通信 (IPC), 自动启动嵌入式 MediaDriver.
     *
     * @param streamId Aeron stream ID
     */
    public static AeronCfg ipc(int streamId) {
        return new AeronCfg(AeronChannel.ipc(), streamId, true);
    }

    /**
     * 跨主机 UDP 单播, 不启动嵌入式 MediaDriver (由外部独立 MediaDriver 提供).
     *
     * @param host     目标主机地址
     * @param port     目标端口
     * @param streamId Aeron stream ID
     */
    public static AeronCfg udp(@Nonnull String host, int port, int streamId) {
        nonEmpty(host, "host");
        return new AeronCfg(AeronChannel.udp(host, port), streamId, false);
    }

    private final AeronChannel channel;
    private final int streamId;
    private final boolean embeddedDriver;

    private AeronCfg(AeronChannel channel, int streamId, boolean embeddedDriver) {
        this.channel = channel;
        this.streamId = streamId;
        this.embeddedDriver = embeddedDriver;
    }

    AeronChannel getChannel() {
        return channel;
    }

    int getStreamId() {
        return streamId;
    }

    /**
     * 创建新的 {@link Aeron} 客户端. 若 {@code embeddedDriver=true}, 确保共享 MediaDriver 已启动.
     * 每个组件调用一次, 返回独立客户端连接.
     */
    Aeron newAeron() {
        Aeron.Context ctx = new Aeron.Context();
        if (embeddedDriver) {
            MediaDriver driver = SHARED_DRIVER.updateAndGet(
                    d -> d != null ? d : MediaDriver.launchEmbedded()
            );
            ctx.aeronDirectoryName(driver.aeronDirectoryName());
            log.debug("Aeron client connecting to embedded driver dir: {}", driver.aeronDirectoryName());
        }
        return Aeron.connect(ctx);
    }

    /**
     * 创建发布者, 发布到本配置的 channel 和 streamId.
     *
     * @param serializer 消息序列化器
     */
    public <T> AeronPublisher<T> createPublisher(@Nonnull BytesSerializer<T> serializer) {
        nonNull(serializer, "serializer");
        return new AeronPublisher<>(this, serializer);
    }

    /**
     * 创建订阅者, 订阅本配置 channel 上的多个 stream.
     *
     * @param streamIds 要订阅的 stream ID 数组
     * @param consumer  消息消费者, 参数为 (streamId, payload)
     */
    public AeronSubscriber createSubscriber(@Nonnull int[] streamIds,
                                            @Nonnull BiConsumer<Integer, byte[]> consumer) {
        nonNull(streamIds, "streamIds");
        nonNull(consumer, "consumer");
        return new AeronSubscriber(this, streamIds, consumer);
    }

    @Override
    public String getConnectionInfo() {
        return channel.uri() + "#" + streamId;
    }

    @Override
    public String getConfigInfo() {
        return "AeronCfg{channel=" + channel.uri() + ", streamId=" + streamId
                + ", embedded=" + embeddedDriver + "}";
    }

    @Override
    public String toString() {
        return getConfigInfo();
    }

}