package io.flatf.transport.api;

@FunctionalInterface
public interface IndexedMessageConsumer<M> {

    void accept(int index, M message);

}
