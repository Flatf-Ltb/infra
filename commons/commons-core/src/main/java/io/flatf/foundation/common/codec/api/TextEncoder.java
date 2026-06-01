package io.flatf.foundation.common.codec.api;

@FunctionalInterface
public interface TextEncoder<T, C extends CharSequence> extends Encoder<T, C> {
}