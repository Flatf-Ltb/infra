package io.flatf.foundation.actor.test;

import io.flatf.foundation.actors.Actor;
import io.flatf.foundation.actors.IActorRef;
import io.flatf.foundation.actors.IActorSystem;
import io.flatf.foundation.actors.Schedulers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.flatf.foundation.actor.test.BasicTest.CheckPoints.ActorCallReturning;
import static io.flatf.foundation.actor.test.BasicTest.CheckPoints.ActorCallSimple;
import static io.flatf.foundation.actor.test.BasicTest.CheckPoints.ActorCallThrowing;
import static io.flatf.foundation.actor.test.BasicTest.CheckPoints.ActorConstructor;
import static io.flatf.foundation.actor.test.BasicTest.CheckPoints.ActorDestructor;
import static io.flatf.foundation.actor.test.BasicTest.CheckPoints.ExceptionHandler;
import static io.flatf.foundation.actor.test.BasicTest.CheckPoints.FutureCompleted;
import static io.flatf.foundation.actor.test.BasicTest.CheckPoints.FutureFailed;
import static io.flatf.foundation.actor.test.BasicTest.CheckPoints.NonActorCallback;
import static io.flatf.foundation.actor.test.BasicTest.CheckPoints.ResultReturned;
import static io.flatf.foundation.actor.test.BasicTest.CheckPoints.validate;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

class BasicTest {

    private final IActorSystem system = Actor.newSystem("kilim", Schedulers.newThreadPerActorScheduler());

    private final IActorRef<Master> master = system.<Master>actorBuilder()
            .constructor(Master::new)
            .build();
    private IActorRef<TestActor> testActor;

    @BeforeAll
    public void before() {
        CheckPoints.clean();
        testActor = system.<TestActor>actorBuilder()
                .constructor(TestActor::new)
                .destructor(TestActor::destructor)
                .exceptionHandler(TestActor::err)
                .build();
    }

    @Test
    public void tell() {
        testActor.tell(TestActor::simple);
        system.shutdownCompletable().join();
        validate(ActorConstructor, ActorCallSimple, ActorDestructor);
    }

    @Test
    public void ask() {
        assertNull(Actor.caller());
        assertNull(Actor.current());
        master.tell(Master::ask47);
        system.shutdownCompletable().join();
        validate(ActorConstructor, ActorCallReturning, ResultReturned, ActorDestructor);
    }

    @Test
    public void askFuture() {
        assertNull(Actor.caller());
        assertNull(Actor.current());
        master.tell(Master::askFuture);
        system.shutdownCompletable().join();
        validate(ActorConstructor, ActorCallReturning, ResultReturned, FutureCompleted, ActorDestructor);
    }

    @Test
    public void askFromNonActor() {
        testActor.ask(TestActor::returning, val -> {
            assertEquals(Integer.valueOf(47), val);
            NonActorCallback.check();
            assertNull(Actor.caller());
            system.shutdown();
        });
        system.shutdownCompletable().join();
        validate(ActorConstructor, ActorCallReturning, NonActorCallback, ActorDestructor);
    }

    @Test
    public void askFutureFromNonActor() {
        CompletableFuture<Integer> future = testActor.ask(TestActor::returning);
        future.thenAccept(val -> {
            assertEquals(Integer.valueOf(47), val);
            NonActorCallback.check();
            assertNull(Actor.caller());
            system.shutdown();
        }).join();
        system.shutdownCompletable().join();
        validate(ActorConstructor, ActorCallReturning, NonActorCallback, ActorDestructor);
    }

    @Test
    public void tellError() {
        master.tell(Master::tellError);
        system.shutdownCompletable().join();
        validate(ActorConstructor, ActorCallThrowing, ExceptionHandler, ActorDestructor);
    }

    @Test
    public void askError() {
        master.tell(Master::askError);
        system.shutdownCompletable().join();
        validate(ActorConstructor, ActorCallThrowing, ExceptionHandler, ActorDestructor);
    }

    @Test
    public void askErrorFuture() {
        master.tell(Master::askErrorFuture);
        system.shutdownCompletable().join();
        validate(ActorConstructor, ActorCallThrowing, FutureFailed, ActorDestructor);
    }

    private class Master {

        void ask47() {
            assertNull(Actor.caller());
            assertEquals(master, Actor.current());
            testActor.ask(TestActor::returning, this::validateResult);
            assertNull(Actor.caller());
            assertEquals(master, Actor.current());
        }

        void askFuture() {
            CompletableFuture<Integer> future = testActor.ask(TestActor::returning);
            assertNull(Actor.caller());
            assertEquals(master, Actor.current());

            future.thenAccept(value -> {
                validateResult(value);
                FutureCompleted.check();
            }).exceptionally(ex -> {
                FutureFailed.check();
                system.shutdown();
                return null;
            });
        }

        void askErrorFuture() {
            CompletableFuture<Integer> future = testActor.ask(TestActor::throwing);
            assertNull(Actor.caller());
            assertEquals(master, Actor.current());

            future.thenAccept(value -> fail()).exceptionally(ex -> {
                assertEquals(master, Actor.current());
                assertEquals(testActor, Actor.caller());
                FutureFailed.check();
                system.shutdown();
                return null;
            });
        }

        void askError() {
            testActor.ask(TestActor::throwing, value -> fail());
            assertNull(Actor.caller());
            assertEquals(master, Actor.current());
        }

        void tellError() {
            testActor.tell(TestActor::throwing);
            assertNull(Actor.caller());
            assertEquals(master, Actor.current());
        }

        private void validateResult(int result) {
            assertEquals(47, result);
            assertEquals(master, Actor.current());
            assertEquals(testActor, Actor.caller());
            ResultReturned.check();
            system.shutdown();
        }
    }

    private class TestActor {

        TestActor() {
            assertEquals(testActor, Actor.current());
            ActorConstructor.check();
        }

        void simple() {
            assertEquals(testActor, Actor.current());
            ActorCallSimple.check();
            system.shutdown();
        }

        int returning() {
            assertEquals(testActor, Actor.current());
            CheckPoints.ActorCallReturning.check();
            return 47;
        }

        int throwing() {
            CheckPoints.ActorCallThrowing.check();
            throw new RuntimeException("oops");
        }

        void err(Exception e) {
            assertEquals(testActor, Actor.current());
            ExceptionHandler.check();
            system.shutdown();
        }

        void destructor() {
            assertEquals(testActor, Actor.current());
            ActorDestructor.check();
        }

    }

    enum CheckPoints {
        ActorConstructor,
        ActorCallSimple,
        ActorCallReturning,
        ActorCallThrowing,
        ActorDestructor,
        ResultReturned,
        ExceptionHandler,
        FutureCompleted,
        FutureFailed,
        NonActorCallback;

        private static final List<CheckPoints> checkpoints = new ArrayList<>();

        static void clean() {
            checkpoints.clear();
        }

        void check() {
            checkpoints.add(this);
        }

        static void validate(CheckPoints... reference) {
            assertEquals(Arrays.asList(reference), checkpoints);
        }
    }

}

