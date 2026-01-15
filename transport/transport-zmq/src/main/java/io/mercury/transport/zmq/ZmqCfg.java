package io.mercury.transport.zmq;

import com.typesafe.config.Config;
import io.mercury.common.annotation.OnlyOverrideEquals;
import io.mercury.common.config.ConfigWrapper;
import io.mercury.common.lang.Validator;
import io.mercury.common.net.IpAddressIllegalException;
import io.mercury.common.net.IpAddressValidator;
import io.mercury.common.serialization.specific.BytesSerializer;
import io.mercury.common.serialization.specific.JsonDeserializable;
import io.mercury.common.serialization.specific.JsonSerializable;
import io.mercury.common.util.StringSupport;
import io.mercury.serialization.json.JsonReader;
import io.mercury.serialization.json.JsonWriter;
import io.mercury.transport.TransportCfg;
import io.mercury.transport.attr.TcpKeepAlive;
import io.mercury.transport.attr.Topics;
import org.slf4j.Logger;
import org.zeromq.ZMQ;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.mercury.common.lang.Validator.greaterThan;
import static io.mercury.common.lang.Validator.nonNull;
import static io.mercury.common.log4j2.Log4j2LoggerFactory.getLogger;
import static io.mercury.common.sys.CurrentRuntime.availableProcessors;
import static io.mercury.transport.zmq.ZmqConfigOption.ADDR;
import static io.mercury.transport.zmq.ZmqConfigOption.IO_THREADS;
import static io.mercury.transport.zmq.ZmqConfigOption.PORT;
import static io.mercury.transport.zmq.ZmqConfigOption.PROTOCOL;

@OnlyOverrideEquals
public final class ZmqCfg implements TransportCfg,
        JsonSerializable, JsonDeserializable<ZmqCfg> {

    private static final Logger log = getLogger(ZmqCfg.class);

    /**
     * @param config Config
     * @return ZmqConfigurator
     */
    public static ZmqCfg config(@Nonnull Config config) {
        return config("", config);
    }

    /**
     * @param config String
     * @param module Config
     * @return ZmqConfigurator
     */
    public static ZmqCfg config(String module, @Nonnull Config config) {
        nonNull(config, "config");
        var wrapper = new ConfigWrapper<ZmqConfigOption>(module, config);
        var protocol = ZmqProtocol.of(wrapper.getStringOrThrows(PROTOCOL));
        ZmqCfg conf;
        switch (protocol) {
            // 使用tcp协议
            case TCP -> {
                int port = wrapper.getIntOrThrows(PORT);
                if (wrapper.hasOption(ADDR)) {
                    var tcpAddr = wrapper.getStringOrThrows(ADDR);
                    Validator.isValid(tcpAddr, IpAddressValidator::isIpAddress,
                            new IpAddressIllegalException(tcpAddr));
                    conf = new ZmqCfg(ZmqAddr.tcp(tcpAddr, port));
                } else {
                    // 没有addr配置项, 使用本地地址
                    conf = new ZmqCfg(ZmqAddr.tcp(port));
                }
            }
            // 使用ipc或inproc协议
            case IPC, INPROC -> {
                var localAddr = wrapper.getStringOrThrows(ADDR);
                var addr = switch (protocol) {
                    case IPC -> ZmqAddr.ipc(localAddr);
                    case INPROC -> ZmqAddr.inproc(localAddr);
                    default -> null;
                };
                conf = new ZmqCfg(addr);
            }
            default -> throw new UnsupportedOperationException(StringSupport.toString(protocol));
        }
        conf.ioThreads(wrapper.getInt(IO_THREADS, 1));
        log.info("created ZmqConfigurator object -> {}", conf);
        return conf;
    }

    /**
     * 创建[Configurator]
     *
     * @param addr ZmqAddr
     * @return ZmqConfigurator
     */
    public static ZmqCfg addr(@Nonnull ZmqAddr addr) {
        return new ZmqCfg(addr);
    }

    /**
     * 创建TCP协议连接
     *
     * @param port int
     * @return ZmqConfigurator
     */
    public static ZmqCfg tcp(int port) {
        return tcp("*", port);
    }

    /**
     * 创建TCP协议连接
     *
     * @param addr String
     * @param port int
     * @return ZmqConfigurator
     */
    public static ZmqCfg tcp(@Nonnull String addr, int port) {
        return new ZmqCfg(ZmqAddr.tcp(addr, port));
    }

    /**
     * 使用[IPC]协议连接, 用于进程间通信
     *
     * @param addr ZmqConfigurator
     * @return String
     */
    public static ZmqCfg ipc(@Nonnull String addr) {
        return new ZmqCfg(ZmqAddr.ipc(addr));
    }

    /**
     * 使用[INPROC]协议连接, 用于线程间通信
     *
     * @param addr String
     * @return ZmqConfigurator
     */
    public static ZmqCfg inproc(@Nonnull String addr) {
        return new ZmqCfg(ZmqAddr.inproc(addr));
    }

    private final ZmqAddr addr;

    private int ioThreads = 1;

    private int highWaterMark = 8192;

    private TcpKeepAlive tcpKeepAlive = null;

    /**
     * @param addr ZmqAddr
     */
    private ZmqCfg(ZmqAddr addr) {
        this.addr = addr;
    }

    public ZmqAddr getAddr() {
        return addr;
    }

    public int getIoThreads() {
        return ioThreads;
    }

    public int getHighWaterMark() {
        return highWaterMark;
    }

    public TcpKeepAlive getTcpKeepAlive() {
        return tcpKeepAlive;
    }

    @Override
    public String getConnectionInfo() {
        return addr.toString();
    }

    /**
     * @param ioThreads int
     * @return ZmqConfigurator
     */
    public ZmqCfg ioThreads(int ioThreads) {
        greaterThan(ioThreads, 1, "ioThreads");
        this.ioThreads = Math.min(ioThreads, availableProcessors());
        return this;
    }

    /**
     * Set high watermark with socket
     *
     * @param highWaterMark int
     * @return ZmqConfigurator
     */
    public ZmqCfg highWaterMark(int highWaterMark) {
        this.highWaterMark = highWaterMark;
        return this;
    }

    /**
     * @param tcpKeepAlive TcpKeepAlive
     * @return ZmqConfigurator
     */
    public ZmqCfg tcpKeepAlive(@Nonnull TcpKeepAlive tcpKeepAlive) {
        nonNull(tcpKeepAlive, "tcpKeepAlive");
        this.tcpKeepAlive = tcpKeepAlive;
        return this;
    }

    /**
     * @return ZmqSender<byte []>
     */
    public ZmqSender<byte[]> createSender() {
        return createSender(bytes -> bytes);
    }

    /**
     * @param <T>        T type
     * @param serializer BytesSerializer<T>
     * @return ZmqSender<T>
     */
    public <T> ZmqSender<T> createSender(@Nonnull BytesSerializer<T> serializer) {
        nonNull(serializer, "ser");
        return new ZmqSender<>(this, serializer);
    }

    /**
     * @param handler Function<byte[], byte[]>
     * @return ZmqReceiver
     */
    public ZmqReceiver createReceiver(@Nonnull Function<byte[], byte[]> handler) {
        nonNull(handler, "handler");
        return new ZmqReceiver(this, handler);
    }

    /**
     * @param consumer BiConsumer<byte[], byte[]>
     * @return ZmqSubscriber
     */
    public ZmqSubscriber createSubscriber(@Nonnull BiConsumer<byte[], byte[]> consumer) {
        return createSubscriber(Topics.with(""), consumer);
    }

    /**
     * @param topics   Topics
     * @param consumer BiConsumer<byte[], byte[]>
     * @return ZmqSubscriber
     */
    public ZmqSubscriber createSubscriber(@Nonnull Topics topics,
                                          @Nonnull BiConsumer<byte[], byte[]> consumer) {
        nonNull(topics, "topics");
        nonNull(consumer, "consumer");
        return new ZmqSubscriber(this, topics, consumer);
    }

    /**
     * @return ZmqPublisher
     */
    public ZmqPublisher<byte[]> createPublisherWithBinary() {
        return createPublisherWithBinary("");
    }

    /**
     * @param topic String
     * @return ZmqPublisher
     */
    public ZmqPublisher<byte[]> createPublisherWithBinary(@Nonnull String topic) {
        return createPublisher(topic, bytes -> bytes);
    }

    /**
     * @return ZmqPublisher
     */
    public ZmqPublisher<String> createPublisherWithString() {
        return createPublisherWithString("", ZMQ.CHARSET);
    }

    /**
     * @param topic String
     * @return ZmqPublisher
     */
    public ZmqPublisher<String> createPublisherWithString(@Nonnull String topic) {
        return createPublisherWithString(topic, ZMQ.CHARSET);
    }

    /**
     * @param encode Charset
     * @return ZmqPublisher
     */
    public ZmqPublisher<String> createPublisherWithString(@Nonnull Charset encode) {
        return createPublisherWithString("", encode);
    }

    /**
     * @param topic  String
     * @param encode Charset
     * @return ZmqPublisher
     */
    public ZmqPublisher<String> createPublisherWithString(@Nonnull String topic,
                                                          @Nonnull Charset encode) {
        nonNull(encode, "encode");
        return createPublisher(topic, str -> str.getBytes(encode));
    }

    /**
     * @param <T>        T type
     * @param serializer BytesSerializer<T>
     * @return ZmqPublisher
     */
    public <T> ZmqPublisher<T> createPublisher(@Nonnull BytesSerializer<T> serializer) {
        return createPublisher("", serializer);
    }

    /**
     * @param <T>        T type
     * @param topic      String
     * @param serializer BytesSerializer<T>
     * @return ZmqPublisher
     */
    public <T> ZmqPublisher<T> createPublisher(@Nonnull String topic,
                                               @Nonnull BytesSerializer<T> serializer) {
        nonNull(topic, "topic");
        nonNull(serializer, "serializer");
        return new ZmqPublisher<>(this, topic, serializer);
    }

    private transient String toStringCache;

    @Override
    public String toString() {
        if (toStringCache == null)
            this.toStringCache = JsonWriter.toJson(this);
        return toStringCache;
    }

    @Override
    public boolean equals(@Nullable Object obj) {
        if (obj instanceof ZmqCfg o) {
            if (!this.addr.equals(o.getAddr()))
                return false;
            if (this.ioThreads != o.getIoThreads())
                return false;
            if (this.highWaterMark != o.getHighWaterMark())
                return false;
            return this.tcpKeepAlive.equals(o.getTcpKeepAlive());
        } else
            return false;
    }

    @Override
    public String getConfigInfo() {
        return toString();
    }

    public static String getZmqVersion() {
        return ZMQ.getVersionString();
    }

    @Nonnull
    @Override
    public String toJson() {
        return JsonWriter.toJsonHasNulls(this);
    }

    @Nonnull
    @Override
    public ZmqCfg fromJson(@Nonnull String json) {
        Map<String, Object> map = JsonReader.toMap(json);
        var protocol = ZmqProtocol.of((String) map.get("protocol"));
        String addr = (String) map.get("addr");
        int ioThreads = (int) map.get("ioThreads");
        var tcpKeepAlive = JsonReader.toObject((String) map.get("tcpKeepAlive"), TcpKeepAlive.class);
        assert tcpKeepAlive != null;
        return new ZmqCfg(protocol.addr(addr)).ioThreads(ioThreads).tcpKeepAlive(tcpKeepAlive);
    }

    public static void main(String[] args) {
        ZmqCfg configurator = ZmqCfg.tcp("192.168.1.1", 5551)
                .ioThreads(3).tcpKeepAlive(TcpKeepAlive.sysDefault());
        System.out.println(configurator);
        System.out.println(ZmqCfg.getZmqVersion());
        System.out.println(IpAddressValidator.isIPv4("192.168.1.1"));
    }

}
