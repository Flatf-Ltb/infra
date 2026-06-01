package io.flatf.foundation.common.serialization.specific;

import io.flatf.foundation.common.serialization.api.Serializer;

import java.io.File;

@FunctionalInterface
public interface FileSerializer<T> extends Serializer<T, File> {
}