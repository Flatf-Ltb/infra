package io.flatf.actors.impl;

import java.util.Collection;

public class FastRegSet<T> implements IRegSet<T> {

    private final ConcurrentDoublyLinkedList<T> list = new ConcurrentDoublyLinkedList<>();

    @Override
    public IRegistration add(T element) {
        Node<T> node = list.coolAdd(element);
        return () -> {
            while (!node.delete())
                ;
        };
    }

    @Override
    public Collection<T> copy() {
        return list;
    }

}

