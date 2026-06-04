package io.flatf.infra.actor.test;

import io.flatf.infra.actors.Actor;
import io.flatf.infra.actors.IActorRef;
import io.flatf.infra.actors.IActorSystem;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;


class NestedAskTest {

    private final IActorSystem system = Actor.newSystem("nested");

    private final IActorRef<Master> master = system.actorOf(Master::new);
    private final IActorRef<Runner1> runner1 = system.actorOf(Runner1::new);
    private final IActorRef<Runner2> runner2 = system.actorOf(Runner2::new);

    private final IActorRef<FortySeven> fortySeven = system.actorOf(FortySeven::new);

    @Test
    public void testNestedAsk() {
        master.tell(Master::run);
        system.shutdownCompletable().join();
    }

    private class Master {

        void run() {
            runner1.ask(Runner1::run, this::recv);
        }

        private void recv(int result) {
            assertEquals(47, result);
            assertEquals(master, Actor.current());
            assertEquals(runner1, Actor.caller());
            system.shutdown();
        }
    }

    private class Runner1 {
        void run(Consumer<Integer> callback) {
            assertEquals(runner1, Actor.current());
            assertEquals(master, Actor.caller());
            runner2.ask(Runner2::run, i -> {
                assertEquals(runner1, Actor.current());
                assertEquals(runner2, Actor.caller());
                callback.accept(i);
            });
        }
    }

    private class Runner2 {
        void run(Consumer<Integer> callback) {
            assertEquals(runner2, Actor.current());
            assertEquals(runner1, Actor.caller());
            fortySeven.ask((r, c) -> c.accept(r.run()), (Integer i) -> {
                assertEquals(runner2, Actor.current());
                assertEquals(fortySeven, Actor.caller());
                callback.accept(i);
            });
        }
    }

    private class FortySeven {
        int run() {
            assertEquals(fortySeven, Actor.current());
            assertEquals(runner2, Actor.caller());
            return 47;
        }
    }

}

