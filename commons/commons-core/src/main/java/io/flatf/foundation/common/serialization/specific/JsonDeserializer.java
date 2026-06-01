package io.flatf.foundation.common.serialization.specific;

import io.flatf.foundation.common.serialization.api.Deserializer;

@FunctionalInterface
public interface JsonDeserializer<R> extends Deserializer<String, R> {
}