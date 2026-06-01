package io.flatf.foundation.common.codec.api;

@FunctionalInterface
public interface TextDecoder<C extends CharSequence, R> extends Decoder<C, R> {
}