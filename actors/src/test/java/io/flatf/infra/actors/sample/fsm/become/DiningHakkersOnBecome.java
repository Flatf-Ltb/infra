package io.flatf.infra.actors.sample.fsm.become;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import io.flatf.infra.actors.sample.fsm.become.Messages.Busy;
import io.flatf.infra.actors.sample.fsm.become.Messages.Put;
import io.flatf.infra.actors.sample.fsm.become.Messages.Take;
import io.flatf.infra.actors.sample.fsm.become.Messages.Taken;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.flatf.infra.actors.sample.fsm.become.Messages.Eat;
import static io.flatf.infra.actors.sample.fsm.become.Messages.Think;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

// Akka adaptation of
// http://www.dalnefre.com/wp/2010/08/dining-philosophers-in-humus/

public class DiningHakkersOnBecome {

    /*
     * A Chopstick is an actor, it can be taken, and put back
     */
    public static class Chopstick extends AbstractActor {

        // When a Chopstick is taken by a hakker
        // It will refuse to be taken by other hakkers
        // But the owning hakker can put it back
        Receive takenBy(ActorRef hakker) {
            return receiveBuilder()
                    .match(Take.class,
                            take -> take.hakker()
                                    .tell(new Busy(self()), self()))
                    .match(Put.class,
                            put -> put.hakker() == hakker,
                            put -> getContext().become(available)).build();
        }

        // When a Chopstick is available, it can be taken by a hakker
        Receive available = receiveBuilder()
                .match(Take.class,
                        take -> {
                            getContext().become(takenBy(take.hakker()));
                            take.hakker().tell(new Taken(self()), self());
                        }).build();

        // A Chopstick begins its existence as available
        @Override
        public Receive createReceive() {
            return available;
        }
    }

    /*
     * A hakker is an awesome dude or dudette who either thinks about hacking or has
     * to eat ;-)
     */
    public static class Hakker extends AbstractActor {
        private String name;
        private ActorRef left;
        private ActorRef right;

        public Hakker(String name, ActorRef left, ActorRef right) {
            this.name = name;
            this.left = left;
            this.right = right;
        }

        @Override
        public Receive createReceive() {
            // All hakkers start in a non-eating state
            return receiveBuilder()
                    .matchEquals(Think, message -> {
                        System.out.printf("%s starts to think%n", name);
                        startThinking(Duration.create(5, SECONDS));
                    }).build();
        }

        // When a hakker is eating, he can decide to start to think,
        // then he puts down his chopsticks and starts to think
        Receive eating = receiveBuilder()
                .matchEquals(Think, message -> {
                    left.tell(new Put(self()), self());
                    right.tell(new Put(self()), self());
                    System.out.printf("%s puts down his chopsticks and starts to think%n", name);
                    startThinking(Duration.create(5, SECONDS));
                }).build();

        // When a hakker is waiting for the last chopstick it can either obtain it
        // and start eating, or the other chopstick was busy, and the hakker goes
        // back to think about how he should obtain his chopsticks :-)
        Receive waitingFor(ActorRef chopstickToWaitFor, ActorRef otherChopstick) {
            return receiveBuilder()
                    .match(Taken.class,
                            taken -> taken.chopstick() == chopstickToWaitFor,
                            taken -> {
                                System.out.printf("%s has picked up %s and %s and starts to eat%n", name,
                                        left.path().name(), right.path().name());
                                getContext().become(eating);
                                getContext()
                                        .system()
                                        .scheduler()
                                        .scheduleOnce(Duration.create(5, SECONDS), self(), Think,
                                                getContext().system().dispatcher(), self());
                            })
                    .match(Busy.class,
                            busy -> {
                                otherChopstick.tell(new Put(self()), self());
                                startThinking(Duration.create(10, MILLISECONDS));
                            }).build();
        }

        // When the results of the other grab comes back,
        // he needs to put it back if he got the other one.
        // Then go back and think and try to grab the chopsticks again
        Receive deniedAChopstick = receiveBuilder()
                .match(Taken.class,
                        taken -> {
                            taken.chopstick().tell(new Put(self()), self());
                            startThinking(Duration.create(10, MILLISECONDS));
                        })
                .match(Busy.class,
                        busy -> startThinking(Duration.create(10, MILLISECONDS))).build();

        // When a hakker is hungry it tries to pick up its chopsticks and eat
        // When it picks one up, it goes into wait for the other
        // If the hakkers first attempt at grabbing a chopstick fails,
        // it starts to wait for the response of the other grab
        Receive hungry = receiveBuilder()
                .match(Taken.class, taken -> taken.chopstick() == left,
                        taken -> getContext().become(waitingFor(right, left)))
                .match(Taken.class, taken -> taken.chopstick() == right,
                        taken -> getContext().become(waitingFor(left, right)))
                .match(Busy.class, busy -> getContext().become(deniedAChopstick)).build();

        // When a hakker is thinking it can become hungry
        // and try to pick up its chopsticks and eat
        Receive thinking = receiveBuilder()
                .matchEquals(Eat, message -> {
                    getContext().become(hungry);
                    left.tell(new Take(self()), self());
                    right.tell(new Take(self()), self());
                }).build();

        private void startThinking(FiniteDuration duration) {
            getContext().become(thinking);
            getContext()
                    .system()
                    .scheduler()
                    .scheduleOnce(duration, self(), Eat, getContext().system().dispatcher(),
                            self());
        }
    }

    /*
     * Alright, here's our test-harness
     */
    public static void main(String[] args) {
        ActorSystem system = ActorSystem.create();
        // Create 5 chopsticks
        ActorRef[] chopsticks = new ActorRef[5];
        for (int i = 0; i < 5; i++)
            chopsticks[i] = system.actorOf(Props.create(Chopstick.class), "Chopstick" + i);

        // Create 5 awesome hakkers and assign them their left and right chopstick
        List<String> names = Arrays.asList("Ghosh", "Boner", "Klang", "Krasser", "Manie");
        List<ActorRef> hakkers = new ArrayList<>();
        int i = 0;
        for (String name : names) {
            hakkers.add(system.actorOf(Props.create(Hakker.class, name, chopsticks[i], chopsticks[(i + 1) % 5])));
            i++;
        }
        // Signal all hakkers that they should start thinking, and watch the show
        hakkers.forEach(hakker -> hakker.tell(Think, ActorRef.noSender()));
    }
}

