package io.flatf.infra.serialization.fory;

import io.flatf.common.epoch.EpochUnit;
import io.flatf.common.sequence.OrderedObject;
import io.flatf.infra.serialization.ContentType;
import io.flatf.infra.serialization.specific.BytesDeserializable;
import io.flatf.infra.serialization.specific.BytesSerializable;
import org.apache.fory.Fory;
import org.apache.fory.ThreadLocalFory;
import org.apache.fory.ThreadSafeFory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import static org.apache.fory.config.Language.JAVA;

/**
 *
 */
@ThreadSafe
public final class ThreadSafeForyMsg implements OrderedObject<ThreadSafeForyMsg>,
        BytesSerializable, BytesDeserializable<ThreadSafeForyMsg> {

    private static final ThreadSafeFory THREAD_SAFE_FORY = new ThreadLocalFory(ThreadSafeForyMsg::newThreadLocalFory);

    private static Fory newThreadLocalFory(ClassLoader classLoader) {
        var fory = Fory.builder()
                .withLanguage(JAVA)
                .withClassLoader(classLoader)
                .build();
        fory.register(ThreadSafeForyMsg.class);
        return fory;
    }

    private long sequence;
    private long epoch;
    private EpochUnit epochUnit = EpochUnit.MILLIS;
    private int envelope;
    private int version = 1;
    private ContentType contentType;
    private byte[] content;

    public long getSequence() {
        return sequence;
    }

    public ThreadSafeForyMsg setSequence(long sequence) {
        this.sequence = sequence;
        return this;
    }

    public long getEpoch() {
        return epoch;
    }

    public ThreadSafeForyMsg setEpoch(long epoch) {
        this.epoch = epoch;
        return this;
    }

    public EpochUnit getEpochUnit() {
        return epochUnit;
    }

    public ThreadSafeForyMsg setEpochUnit(EpochUnit epochUnit) {
        this.epochUnit = epochUnit;
        return this;
    }

    public int getEnvelope() {
        return envelope;
    }

    public ThreadSafeForyMsg setEnvelope(int envelope) {
        this.envelope = envelope;
        return this;
    }

    public int getVersion() {
        return version;
    }

    public ThreadSafeForyMsg setVersion(int version) {
        this.version = version;
        return this;
    }

    public ContentType getContentType() {
        return contentType;
    }

    public ThreadSafeForyMsg setContentType(ContentType contentType) {
        this.contentType = contentType;
        return this;
    }

    public byte[] getContent() {
        return content;
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
        return THREAD_SAFE_FORY.deserializeJavaObject(bytes, ThreadSafeForyMsg.class);
    }

    @Nonnull
    @Override
    public byte[] toBytes() {
        return THREAD_SAFE_FORY.serializeJavaObject(this);
    }

}
