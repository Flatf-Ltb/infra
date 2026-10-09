/*
 * Copyright (c) 2016-2019 Chronicle Software Ltd
 */

package town.lost.oms.dto;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.wire.Base85LongConverter;
import net.openhft.chronicle.wire.LongConversion;
import net.openhft.chronicle.wire.MicroTimestampLongConverter;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;

import javax.annotation.Nonnull;

public abstract class AbstractEvent<E extends AbstractEvent<E>> extends SelfDescribingMarshallable {

    // used to control the benchmark
    public static final boolean BYTES_IN_BINARY = Boolean.getBoolean("byteInBinary");

    // used to control the benchmark
    public static final boolean PREGENERATED_MARSHALLABLE = Boolean.getBoolean("pregeneratedMarshallable");

    private static final int MASHALLABLE_VERSION = 1;

    @LongConversion(Base85LongConverter.class)
    private long sender;
    @LongConversion(Base85LongConverter.class)
    private long target;
    // client sending time
    @LongConversion(MicroTimestampLongConverter.class)
    private long sendingTime;

    @Override
    public boolean usesSelfDescribingMessage() {
        return !BYTES_IN_BINARY;
    }

    public long sender() {
        return sender;
    }

    public E sender(long sender) {
        this.sender = sender;
        return self();
    }

    public long target() {
        return target;
    }

    public E target(long target) {
        this.target = target;
        return self();
    }

    public long sendingTime() {
        return sendingTime;
    }

    public E sendingTime(long sendingTime) {
        this.sendingTime = sendingTime;
        return self();
    }

    protected abstract E self();

    @Override
    public void writeMarshallable(BytesOut out) {
        if (PREGENERATED_MARSHALLABLE) {
            out.writeStopBit(MASHALLABLE_VERSION);
            out.writeLong(sender);
            out.writeLong(target);
            out.writeLong(sendingTime);
        } else {
            super.writeMarshallable(out);
        }
    }

    @Override
    public void readMarshallable(BytesIn in) {
        if (PREGENERATED_MARSHALLABLE) {
            int version = (int) in.readStopBit();
            if (version == MASHALLABLE_VERSION) {
                sender = in.readLong();
                target = in.readLong();
                sendingTime = in.readLong();
            } else {
                throw new IllegalStateException("Unknown version " + version);
            }
        } else {
            super.readMarshallable(in);
        }
    }

    @Override
    public void writeMarshallable(@Nonnull WireOut out) {
        if (PREGENERATED_MARSHALLABLE) {
            out.write("sender").writeLong(Base85LongConverter.INSTANCE, sender);
            out.write("target").writeLong(Base85LongConverter.INSTANCE, target);
            out.write("sendingTime").writeLong(MicroTimestampLongConverter.INSTANCE, sendingTime);
        } else {
            super.writeMarshallable(out);
        }
    }

    @Override
    public void readMarshallable(@Nonnull WireIn in) {
        if (PREGENERATED_MARSHALLABLE) {
            sender = in.read("sender").readLong(Base85LongConverter.INSTANCE);
            target = in.read("target").readLong(Base85LongConverter.INSTANCE);
            sendingTime = in.read("sendingTime").readLong(MicroTimestampLongConverter.INSTANCE);
        } else {
            super.readMarshallable(in);
        }
    }
}
