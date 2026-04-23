package io.flatf.transport.aeron;

import javax.annotation.Nonnull;

import static io.flatf.common.lang.Validator.nonEmpty;

/**
 * Aeron 通道地址, 对应 ZMQ 中的 {@code ZmqAddr}.
 *
 * <p>支持两种协议:
 * <ul>
 *   <li>{@link Protocol#IPC}  — 进程间通信 (同主机共享 MediaDriver)
 *   <li>{@link Protocol#UDP}  — 网络单播 (跨主机)
 * </ul>
 */
public final class AeronChannel {

    public enum Protocol {IPC, UDP}

    private final Protocol protocol;
    private final String uri;

    private AeronChannel(Protocol protocol, String uri) {
        this.protocol = protocol;
        this.uri = uri;
    }

    /**
     * 同主机进程间通信, 使用共享内存 MediaDriver.
     *
     * @return AeronChannel: {@code aeron:ipc}
     */
    public static AeronChannel ipc() {
        return new AeronChannel(Protocol.IPC, "aeron:ipc");
    }

    /**
     * 跨主机 UDP 单播.
     *
     * @param host 目标主机地址
     * @param port 目标端口
     * @return AeronChannel: {@code aeron:udp?endpoint=host:port}
     */
    public static AeronChannel udp(@Nonnull String host, int port) {
        nonEmpty(host, "host");
        if (port <= 0 || port > 65_535) {
            throw new IllegalArgumentException("port must be between 1 and 65535");
        }
        return new AeronChannel(Protocol.UDP, "aeron:udp?endpoint=" + host + ":" + port);
    }

    public Protocol protocol() {
        return protocol;
    }

    /**
     * 返回完整 Aeron channel URI, 可直接传入 {@code Aeron.addPublication()} 等方法.
     */
    public String uri() {
        return uri;
    }

    @Override
    public String toString() {
        return uri;
    }

}
