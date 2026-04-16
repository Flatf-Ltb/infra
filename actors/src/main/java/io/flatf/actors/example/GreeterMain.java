package io.flatf.actors.example;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import io.flatf.actors.example.msg.Greet;
import io.flatf.actors.example.msg.Greeted;
import io.flatf.actors.example.msg.SayHello;

/**
 * @author <b>Akka official</b><br>
 * <br>
 * <doc> Each actor defines a type T for the messages it can receive.
 * Case classes and case objects make excellent messages since they are
 * immutable and have support for pattern matching, something we will
 * take advantage of in the Actor when matching on the messages it has
 * received.<br>
 * <br>
 * The Hello World Actors use three different messages: <br>
 * <br>
 * Greet: command sent to the Greeter actor to greet Greeted: reply from
 * the Greeter actor to confirm the greeting has happened SayHello:
 * command to the GreeterMain to start the greeting process <br>
 * <br>
 * When defining Actors and their messages, keep these recommendations
 * in mind: <br>
 * <br>
 * Since messages are the Actor鈥檚 public API, it is a good practice to
 * define messages with good names and rich semantic and domain specific
 * meaning, even if they just wrap your data type. This will make it
 * easier to use, understand and debug actor-based systems. <br>
 * <br>
 * Messages should be immutable, since they are shared between different
 * threads. <br>
 * <br>
 * It is a good practice to put an actor鈥檚 associated messages as static
 * classes in the AbstractBehaavior鈥檚 class. This makes it easier to
 * understand what type of messages the actor expects and handles. <br>
 * <br>
 * It is a good practice obtain an actor鈥檚 initial behavior via a static
 * factory method <br>
 * <br>
 * Lets see how the implementations for Greeter, GreeterBot and
 * GreeterMain demonstrate these best practices. <doc>
 */
public class GreeterMain extends AbstractBehavior<SayHello> {

    private final ActorRef<Greet> greeter;

    public static Behavior<SayHello> create() {
        return Behaviors.setup(GreeterMain::new);
    }

    private GreeterMain(ActorContext<SayHello> context) {
        super(context);
        // #create-actors
        greeter = context.spawn(Greeter.create(), "greeter");
        // #create-actors
    }

    @Override
    public Receive<SayHello> createReceive() {
        return newReceiveBuilder().onMessage(SayHello.class, this::onSayHello).build();
    }

    private Behavior<SayHello> onSayHello(SayHello command) {
        // #create-actors
        ActorRef<Greeted> replyTo = getContext().spawn(GreeterBot.create(3), command.name());
        greeter.tell(new Greet(command.name(), replyTo));
        // #create-actors
        return this;
    }

}
