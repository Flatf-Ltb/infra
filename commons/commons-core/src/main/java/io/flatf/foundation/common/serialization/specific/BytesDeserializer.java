package io.flatf.foundation.common.serialization.specific;

import io.flatf.foundation.common.serialization.api.Deserializer;

@FunctionalInterface
public interface BytesDeserializer<R> extends Deserializer<byte[], R> {
}
