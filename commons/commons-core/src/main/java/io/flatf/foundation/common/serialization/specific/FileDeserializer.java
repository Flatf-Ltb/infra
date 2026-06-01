package io.flatf.foundation.common.serialization.specific;

import io.flatf.foundation.common.serialization.api.Deserializer;

import java.io.File;

@FunctionalInterface
public interface FileDeserializer<T> extends Deserializer<File, T> {
}