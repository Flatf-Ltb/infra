package io.flatf.infra.actors.sample.fsm;

import akka.actor.ActorRef;

public class Messages {

    public record Busy(ActorRef chopstick) {
    }

    private interface PutMessage {
    }

    public static final Object Put = new PutMessage() {
        @Override
        public String toString() {
            return "Put";
        }
    };

    private interface TakeMessage {
    }

    public static final Object Take = new TakeMessage() {
        @Override
        public String toString() {
            return "Take";
        }
    };

    public static final class Taken {
        public final ActorRef chopstick;

        public Taken(ActorRef chopstick) {
            this.chopstick = chopstick;
        }
    }

    private interface ThinkMessage {
    }

    public static final Object Think = new ThinkMessage() {
        @Override
        public String toString() {
            return "Think";
        }
    };
}

