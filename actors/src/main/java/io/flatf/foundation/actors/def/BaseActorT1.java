package io.flatf.foundation.actors.def;

public abstract class BaseActorT1<T> extends BaseActor {

    protected final Class<T> type;

    protected BaseActorT1() {
        this.type = eventType();
    }

    @Override
    public final Receive createReceive() {
        return getReceiveBuilder()
                // match type
                .match(type, this::onEvent)
                // build
                .build();
    }

    protected abstract Class<T> eventType();

    protected abstract void onEvent(T t);

}

