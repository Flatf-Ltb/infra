package io.flatf.foundation.transport.aeron;

import org.agrona.DirectBuffer;

import javax.annotation.Nonnull;

import static io.flatf.foundation.common.lang.Validator.nonNull;

/**
 * Callback-scoped zero-copy payload view for Aeron subscriber callbacks.
 *
 * <p>The underlying buffer is owned by Aeron and should only be used during the callback.
 * If the data must outlive the callback, copy it explicitly via {@link #copyBytes()}.
 */
public final class AeronMessageView {

    private DirectBuffer buffer;
    private int offset;
    private int length;

    AeronMessageView wrap(@Nonnull DirectBuffer buffer, int offset, int length) {
        nonNull(buffer, "buffer");
        if (offset < 0) throw new IllegalArgumentException("offset must not be negative");
        if (length < 0) throw new IllegalArgumentException("length must not be negative");
        this.buffer = buffer;
        this.offset = offset;
        this.length = length;
        return this;
    }

    public DirectBuffer buffer() {
        return buffer;
    }

    public int offset() {
        return offset;
    }

    public int length() {
        return length;
    }

    public byte getByte(int index) {
        validateRelativeIndex(index, 1);
        return buffer.getByte(offset + index);
    }

    public void copyTo(@Nonnull byte[] target) {
        copyTo(target, 0);
    }

    public void copyTo(@Nonnull byte[] target, int targetOffset) {
        nonNull(target, "target");
        if (targetOffset < 0) throw new IllegalArgumentException("targetOffset must not be negative");
        if (target.length - targetOffset < length) {
            throw new IllegalArgumentException("target does not have enough remaining capacity");
        }
        buffer.getBytes(offset, target, targetOffset, length);
    }

    public byte[] copyBytes() {
        byte[] copy = new byte[length];
        copyTo(copy);
        return copy;
    }

    private void validateRelativeIndex(int index, int accessLength) {
        if (index < 0 || index + accessLength > length) {
            throw new IndexOutOfBoundsException("index=" + index + ", length=" + length);
        }
    }
}
