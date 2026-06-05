package io.flatf.infra.serialization.fory;

import io.flatf.common.epoch.EpochUnit;
import io.flatf.common.sequence.OrderedObject;
import io.flatf.infra.serialization.ContentType;
import lombok.Getter;
import org.apache.fory.Fory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

@Getter
@NotThreadSafe
public class ForyMsg implements OrderedObject<ForyMsg> {

    private long sequence;
    private long epoch;
    private EpochUnit epochUnit = EpochUnit.MILLIS;
    private int envelope;
    private int version = 1;
    private ContentType contentType;
    private byte[] content;

    public ForyMsg setSequence(long sequence) {
        this.sequence = sequence;
        return this;
    }

    public ForyMsg setEpoch(long epoch) {
        this.epoch = epoch;
        return this;
    }

    public ForyMsg setEpoch(long epoch, EpochUnit epochUnit) {
        this.epoch = epoch;
        this.epochUnit = epochUnit;
        return this;
    }

    public ForyMsg setEpochUnit(EpochUnit epochUnit) {
        this.epochUnit = epochUnit;
        return this;
    }

    public ForyMsg setEnvelope(int envelope) {
        this.envelope = envelope;
        return this;
    }

    public ForyMsg setVersion(int version) {
        this.version = version;
        return this;
    }

    public ForyMsg setContentType(ContentType contentType) {
        this.contentType = contentType;
        return this;
    }

    public ForyMsg setContent(byte[] content) {
        this.content = content;
        return this;
    }

    @Override
    public long sequence() {
        return sequence;
    }

    @Nonnull
    public byte[] toBytes(@Nonnull Fory fory) {
        return fory.serialize(this);
    }

    @Nonnull
    public static ForyMsg fromBytes(@Nonnull Fory fory, @Nonnull byte[] bytes) {
        return fory.deserialize(bytes, ForyMsg.class);
    }

}
