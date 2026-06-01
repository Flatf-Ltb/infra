package io.flatf.foundation.actors.def;

public abstract class BaseActorT3<T0, T1, T2> extends BaseActor {

    protected final Class<T0> type0;
    protected final Class<T1> type1;
    protected final Class<T2> type2;

    protected BaseActorT3() {
        this.type0 = eventType0();
        this.type1 = eventType1();
        this.type2 = eventType2();
    }

    @Override
    public final Receive createReceive() {
        return getReceiveBuilder()
                // match type0
                .match(type0, this::onEvent0)
                // match type1
                .match(type1, this::onEvent1)
                // match type2
                .match(type2, this::onEvent2)
                // build
                .build();
    }

    protected abstract Class<T0> eventType0();

    protected abstract Class<T1> eventType1();

    protected abstract Class<T2> eventType2();

    protected abstract void onEvent0(T0 t0);

    protected abstract void onEvent1(T1 t1);

    protected abstract void onEvent2(T2 t2);

}

