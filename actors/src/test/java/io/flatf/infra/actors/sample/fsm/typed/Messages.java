package io.flatf.infra.actors.sample.fsm.typed;

import akka.actor.typed.ActorRef;

public class Messages {


    interface ChopstickMessage {

        record Take(ActorRef<ChopstickAnswer> hakker) implements ChopstickMessage {
        }

        record Put(ActorRef<ChopstickAnswer> hakker) implements ChopstickMessage {
        }
    }

    interface ChopstickAnswer {

        ActorRef<ChopstickMessage> chopstick();

        default boolean isTakenBy() {
            return false;
        }

        default boolean isTakenBy(ActorRef<ChopstickMessage> chopstick) {
            return false;
        }

        default boolean isBusy() {
            return false;
        }

        default boolean isBusy(ActorRef<ChopstickMessage> chopstick) {
            return false;
        }

        record Taken(ActorRef<ChopstickMessage> chopstick) implements ChopstickAnswer {

            @Override
            public boolean isTakenBy(ActorRef<ChopstickMessage> chopstick) {
                return this.chopstick.equals(chopstick);
            }

            @Override
            public boolean isTakenBy() {
                return true;
            }
        }

        record Busy(ActorRef<ChopstickMessage> chopstick) implements ChopstickAnswer {

            @Override
            public boolean isBusy() {
                return true;
            }

            @Override
            public boolean isBusy(ActorRef<ChopstickMessage> chopstick) {
                return this.chopstick.equals(chopstick);
            }
        }
    }

    interface HakkerMessage {

        enum Eat implements HakkerMessage {
            INSTANCE
        }

        enum Think implements HakkerMessage {
            INSTANCE
        }
    }

}

