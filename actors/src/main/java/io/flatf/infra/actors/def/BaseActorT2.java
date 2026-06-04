package io.flatf.infra.actors.def;

public abstract class BaseActorT2<T0, T1> extends BaseActor {

    protected final Class<T0> type0;
    protected final Class<T1> type1;

    protected BaseActorT2() {
        this.type0 = eventType0();
        this.type1 = eventType1();
    }

    @Override
    public final Receive createReceive() {
        return getReceiveBuilder()
                // match type0
                .match(type0, this::onEvent0)
                // match type1
                .match(type1, this::onEvent1)
                // build
                .build();
    }

    protected abstract Class<T0> eventType0();

    protected abstract Class<T1> eventType1();

    protected abstract void onEvent0(T0 t0);

    protected abstract void onEvent1(T1 t1);

}

