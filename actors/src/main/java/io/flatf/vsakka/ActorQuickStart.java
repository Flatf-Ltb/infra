package io.flatf.vsakka;

import io.flatf.actors.Actor;
import io.flatf.actors.IActorRef;
import io.flatf.actors.IActorSystem;

import java.io.IOException;

public class ActorQuickStart {

    public static void main(String[] args) {
        final IActorSystem system = Actor.newSystem("hello-actor");
        try (// #create-actors
             final IActorRef<Printer> printerActor = system.actorOf(Printer::new, "printerActor");
             final IActorRef<Greeter> howdyGreeter = system.actorOf(() -> new Greeter("Howdy", printerActor), "howdyGreeter");
             final IActorRef<Greeter> helloGreeter = system.actorOf(() -> new Greeter("Hello", printerActor), "helloGreeter");
             final IActorRef<Greeter> goodDayGreeter = system.actorOf(() -> new Greeter("Good day", printerActor), "goodDayGreeter")
             // #create-actors
        ) {
            // #main-send-messages
            howdyGreeter.tell(greeter -> greeter.setWhoToGreet("Actor"));
            howdyGreeter.tell(Greeter::greet);

            howdyGreeter.tell(greeter -> greeter.setWhoToGreet("Mercury"));
            howdyGreeter.tell(Greeter::greet);

            helloGreeter.tell(greeter -> greeter.setWhoToGreet("Java"));
            helloGreeter.tell(Greeter::greet);

            goodDayGreeter.tell(greeter -> greeter.setWhoToGreet("Lambda"));
            goodDayGreeter.tell(Greeter::greet);
            // #main-send-messages

            System.out.println(">>> Press ENTER to exit <<<");

            System.in.read();
        } catch (IOException ignored) {
        } finally {
            system.shutdown();
        }
    }

}

