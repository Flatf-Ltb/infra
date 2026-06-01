package io.flatf.foundation.common.serialization.specific;

import io.flatf.foundation.common.serialization.api.Serializer;

@FunctionalInterface
public interface BytesSerializer<T> extends Serializer<T, byte[]> {
}