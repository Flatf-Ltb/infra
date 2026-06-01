package io.flatf.foundation.actors.example;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import io.flatf.foundation.actors.example.msg.Greet;
import io.flatf.foundation.actors.example.msg.Greeted;

/**
 * @author Akka official
 */
public class Greeter extends AbstractBehavior<Greet> {

    public static Behavior<Greet> create() {
        return Behaviors.setup(Greeter::new);
    }

    private Greeter(ActorContext<Greet> context) {
        super(context);
    }

    @Override
    public Receive<Greet> createReceive() {
        return newReceiveBuilder().onMessage(Greet.class, this::onGreet).build();
    }

    private Behavior<Greet> onGreet(Greet command) {
        getContext().getLog().info("Hello {}!", command.whom());
        // #greeter-send-message
        command.replyTo().tell(new Greeted(command.whom(), getContext().getSelf()));
        // #greeter-send-message
        return this;
    }
}

