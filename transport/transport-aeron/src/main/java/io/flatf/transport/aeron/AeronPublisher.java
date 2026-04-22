package io.flatf.transport.aeron;

import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.flatf.common.serialization.specific.BytesSerializer;
import io.flatf.transport.api.Publisher;
import io.flatf.transport.exception.PublishFailedException;
import org.agrona.CloseHelper;
import org.agrona.ExpandableArrayBuffer;
import org.slf4j.Logger;

import javax.annotation.Nonnull;

import static io.flatf.common.lang.Validator.nonNull;
import static io.flatf.common.log4j2.Log4j2LoggerFactory.getLogger;

/**
 * Aeron 发布者, 对应 ZMQ 中的 {@code ZmqPublisher}.
 *
 * <p>持有单个 {@link ExclusivePublication}, 绑定到 {@link AeronCfg} 指定的 channel 和 streamId.
 * 使用 {@link ExpandableArrayBuffer} 复用写缓冲, 减少 GC 压力.
 *
 * <p>线程安全: 本类仅允许单线程调用 {@link #publish}, 与 {@link ExclusivePublication} 的语义一致.
 */
public final class AeronPublisher<T> extends AeronComponent implements Publisher<Integer, T> {

    private static final Logger log = getLogger(AeronPublisher.class);

    private final ExclusivePublication publication;
    private final BytesSerializer<T> serializer;
    private final ExpandableArrayBuffer writeBuffer = new ExpandableArrayBuffer(4096);

    AeronPublisher(@Nonnull AeronCfg cfg, @Nonnull BytesSerializer<T> serializer) {
        super(cfg);
        nonNull(serializer, "serializer");
        this.serializer = serializer;
        this.publication = aeron.addExclusivePublication(cfg.getChannel().uri(), cfg.getStreamId());
        this.name = "APub$" + cfg.getChannel() + "#" + cfg.getStreamId();
        newStartTime();
        log.info("AeronPublisher created -> {}", name);
    }

    @Override
    public AeronType getAeronType() {
        return AeronType.AERON_PUBLISHER;
    }

    @Override
    protected void closeResources() {
        CloseHelper.quietClose(publication);
    }

    /**
     * 发布到配置的默认 streamId.
     */
    @Override
    public void publish(@Nonnull T msg) throws PublishFailedException {
        publish(cfg.getStreamId(), msg);
    }

    /**
     * 发布到指定 streamId. 注意: 本 publisher 绑定的 streamId 是固定的,
     * 传入的 streamId 参数仅用于与配置校验, 不可动态切换.
     */
    @Override
    public void publish(@Nonnull Integer streamId, @Nonnull T msg) throws PublishFailedException {
        if (!isRunning.get()) {
            log.warn("AeronPublisher -> [{}] already closed", name);
            return;
        }
        byte[] bytes = serializer.serialize(msg);
        if (bytes == null || bytes.length == 0) {
            return;
        }
        writeBuffer.checkLimit(bytes.length);
        writeBuffer.putBytes(0, bytes);
        long result = publication.offer(writeBuffer, 0, bytes.length);
        if (result < 0 && result != Publication.NOT_CONNECTED) {
            log.warn("AeronPublisher -> [{}] offer failed, result={}", name, result);
        }
    }

}