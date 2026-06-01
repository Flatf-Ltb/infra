package io.flatf.foundation.common.functional;

import io.flatf.foundation.common.annotation.thread.MustBeThreadSafe;

import java.util.function.Consumer;

@FunctionalInterface
public interface Handler<E> extends Consumer<E> {

    @MustBeThreadSafe
    void handle(E e);

    @Override
    default void accept(E e) {
        handle(e);
    }

}
