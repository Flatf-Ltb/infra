package io.mercury.serialization.fory;

import io.mercury.common.epoch.EpochUnit;
import io.mercury.common.sequence.OrderedObject;
import io.mercury.common.serialization.ContentType;
import org.apache.fory.Fory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class ForyMsg implements OrderedObject<ForyMsg> {

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

    public ForyMsg setSequence(long sequence) {
        this.sequence = sequence;
        return this;
    }

    public long getEpoch() {
        return epoch;
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

    public EpochUnit getEpochUnit() {
        return epochUnit;
    }

    public ForyMsg setEpochUnit(EpochUnit epochUnit) {
        this.epochUnit = epochUnit;
        return this;
    }

    public int getEnvelope() {
        return envelope;
    }

    public ForyMsg setEnvelope(int envelope) {
        this.envelope = envelope;
        return this;
    }

    public int getVersion() {
        return version;
    }

    public ForyMsg setVersion(int version) {
        this.version = version;
        return this;
    }

    public ContentType getContentType() {
        return contentType;
    }

    public ForyMsg setContentType(ContentType contentType) {
        this.contentType = contentType;
        return this;
    }

    public byte[] getContent() {
        return content;
    }

    public ForyMsg setContent(byte[] content) {
        this.content = content;
        return this;
    }

    @Override
    public long orderNum() {
        return sequence;
    }

    @Nonnull
    public byte[] toBytes(@Nonnull Fory fory) {
        return fory.serializeJavaObject(this);
    }

    @Nonnull
    public static ForyMsg fromBytes(@Nonnull Fory fory, @Nonnull byte[] bytes) {
        return fory.deserializeJavaObject(bytes, ForyMsg.class);
    }

}
