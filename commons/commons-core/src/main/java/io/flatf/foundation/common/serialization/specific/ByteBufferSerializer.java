package io.flatf.foundation.common.serialization.specific;

import io.flatf.foundation.common.serialization.api.Serializer;

import java.nio.ByteBuffer;

@FunctionalInterface
public interface ByteBufferSerializer<T> extends Serializer<T, ByteBuffer> {
}
