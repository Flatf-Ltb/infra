package io.flatf.infra.transport.aeron;

import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.flatf.infra.serialization.specific.BytesSerializer;
import io.flatf.infra.transport.api.Publisher;
import io.flatf.infra.transport.exception.PublishFailedException;
import org.agrona.CloseHelper;
import org.agrona.ExpandableArrayBuffer;
import org.slf4j.Logger;

import javax.annotation.Nonnull;

import static io.flatf.common.lang.Validator.nonNull;
import static io.flatf.common.log4j2.Log4j2LoggerFactory.getLogger;

/**
 * Aeron publisher bound to a single channel and stream.
 */
public final class AeronPublisher<T> extends AeronComponent implements Publisher<Integer, T> {

    private static final Logger log = getLogger(AeronPublisher.class);

    private final ExclusivePublication publication;
    private final BytesSerializer<T> serializer;
    private final ExpandableArrayBuffer writeBuffer = new ExpandableArrayBuffer(4096);

    AeronPublisher(@Nonnull AeronConfig cfg, @Nonnull BytesSerializer<T> serializer) {
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

    @Override
    protected boolean isTransportConnected() {
        return publication != null && !publication.isClosed() && publication.isConnected();
    }

    @Override
    public boolean supportsTargetedPublish() {
        return false;
    }

    @Override
    public void publish(@Nonnull T msg) throws PublishFailedException {
        publish(cfg.getStreamId(), msg);
    }

    @Override
    public void publish(@Nonnull Integer streamId, @Nonnull T msg) throws PublishFailedException {
        if (!Integer.valueOf(cfg.getStreamId()).equals(streamId)) {
            throw new IllegalArgumentException("AeronPublisher is bound to streamId=" + cfg.getStreamId());
        }
        if (!isRunning.get()) {
            throw new PublishFailedException("AeronPublisher already closed -> " + name);
        }

        byte[] bytes = serializer.serialize(msg);
        if (bytes == null || bytes.length == 0) {
            return;
        }

        writeBuffer.checkLimit(bytes.length);
        writeBuffer.putBytes(0, bytes);

        long result = Publication.CLOSED;
        for (int attempt = 1; attempt <= cfg.getPublishRetryCount(); attempt++) {
            result = publication.offer(writeBuffer, 0, bytes.length);
            if (result >= 0) {
                return;
            }
            if (!isRetryable(result) || attempt == cfg.getPublishRetryCount()) {
                throw new PublishFailedException(buildOfferFailureMessage(result, streamId, bytes.length, attempt));
            }
            Thread.onSpinWait();
        }
        throw new PublishFailedException(buildOfferFailureMessage(result, streamId, bytes.length, cfg.getPublishRetryCount()));
    }

    static boolean isRetryable(long result) {
        return result == Publication.BACK_PRESSURED
            || result == Publication.ADMIN_ACTION
            || result == Publication.NOT_CONNECTED;
    }

    static String describeOfferResult(long result) {
        if (result == Publication.NOT_CONNECTED) return "NOT_CONNECTED";
        if (result == Publication.ADMIN_ACTION) return "ADMIN_ACTION";
        if (result == Publication.BACK_PRESSURED) return "BACK_PRESSURED";
        if (result == Publication.CLOSED) return "CLOSED";
        if (result == Publication.MAX_POSITION_EXCEEDED) return "MAX_POSITION_EXCEEDED";
        return "UNKNOWN(" + result + ")";
    }

    private String buildOfferFailureMessage(long result, int streamId, int messageLength, int attempts) {
        return "AeronPublisher offer failed -> name=" + name
            + ", streamId=" + streamId
            + ", bytes=" + messageLength
            + ", attempts=" + attempts
            + ", result=" + describeOfferResult(result);
    }
}
