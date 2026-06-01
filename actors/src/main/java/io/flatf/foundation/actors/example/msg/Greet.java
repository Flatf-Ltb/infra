package io.flatf.foundation.actors.example.msg;

import akka.actor.typed.ActorRef;

/**
 * @author Akka official
 */
public record Greet(
        String whom,
        ActorRef<Greeted> replyTo) {
}
