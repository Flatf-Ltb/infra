package io.flatf.foundation.common.serialization.specific;

import io.flatf.foundation.common.serialization.api.Deserializer;

import java.nio.ByteBuffer;

@FunctionalInterface
public interface ByteBufferDeserializer<R> extends Deserializer<ByteBuffer, R> {
}