package io.flatf.infra.serialization.fory;

import io.flatf.common.epoch.EpochUnit;
import io.flatf.common.sequence.OrderedObject;
import io.flatf.infra.serialization.ContentType;
import io.flatf.infra.serialization.specific.BytesDeserializable;
import io.flatf.infra.serialization.specific.BytesSerializable;
import lombok.Getter;
import org.apache.fory.Fory;
import org.apache.fory.ThreadSafeFory;
import org.apache.fory.config.Language;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

/**
 *
 */
@Getter
@ThreadSafe
public final class ThreadSafeForyMsg
    implements OrderedObject<ThreadSafeForyMsg>,
    BytesSerializable, BytesDeserializable<ThreadSafeForyMsg> {

    private static final ThreadSafeFory THREAD_SAFE_FORY = Fory.builder()
        .withLanguage(Language.JAVA)
        .buildThreadLocalFory();


    private long sequence;
    private long epoch;
    private EpochUnit epochUnit = EpochUnit.MILLIS;
    private int envelope;
    private int version = 1;
    private ContentType contentType;
    private byte[] content;

    public ThreadSafeForyMsg setSequence(long sequence) {
        this.sequence = sequence;
        return this;
    }

    public ThreadSafeForyMsg setEpoch(long epoch) {
        this.epoch = epoch;
        return this;
    }

    public ThreadSafeForyMsg setEpochUnit(EpochUnit epochUnit) {
        this.epochUnit = epochUnit;
        return this;
    }

    public ThreadSafeForyMsg setEnvelope(int envelope) {
        this.envelope = envelope;
        return this;
    }

    public ThreadSafeForyMsg setVersion(int version) {
        this.version = version;
        return this;
    }

    public ThreadSafeForyMsg setContentType(ContentType contentType) {
        this.contentType = contentType;
        return this;
    }

    public ThreadSafeForyMsg setContent(byte[] content) {
        this.content = content;
        return this;
    }

    @Override
    public long sequence() {
        return sequence;
    }

    @Nonnull
    @Override
    public ThreadSafeForyMsg fromBytes(@Nonnull byte[] bytes) {
        return THREAD_SAFE_FORY.deserialize(bytes, ThreadSafeForyMsg.class);
    }

    @Nonnull
    @Override
    public byte[] toBytes() {
        return THREAD_SAFE_FORY.serialize(this);
    }

}
