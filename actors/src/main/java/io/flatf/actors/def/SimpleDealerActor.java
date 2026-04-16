package io.flatf.actors.def;

import akka.actor.ActorRef;
import org.eclipse.collections.api.list.MutableList;

import static io.flatf.common.collections.MutableLists.newFastList;

public abstract class SimpleDealerActor<T> extends BaseActorT2<ActorRef, T> {

    private final MutableList<ActorRef> registered;
    private int turn = -1;

    protected SimpleDealerActor() {
        this.registered = newFastList(8);
    }

    @Override
    protected Class<ActorRef> eventType0() {
        return ActorRef.class;
    }

    @Override
    protected void onEvent0(ActorRef ref) {
        registered.add(ref);
    }

    @Override
    protected void onEvent1(T t) {
        if (registered.isEmpty())
            handleRegisteredIsEmpty(t);
        else
            registered.get(nextActorIndex()).forward(t, getContext());
    }

    private void handleRegisteredIsEmpty(T t) {
        // TODO
    }

    private int nextActorIndex() {
        if (++turn == registered.size())
            turn = 0;
        return turn;
    }

}

