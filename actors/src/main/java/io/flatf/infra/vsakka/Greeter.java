package io.flatf.infra.vsakka;

import io.flatf.infra.actors.IActorRef;

public class Greeter {

    private final String message;
    private final IActorRef<Printer> printerActorRef;
    private String greeting;

    public Greeter(String message, IActorRef<Printer> printerIActorRef) {
        this.message = message;
        this.printerActorRef = printerIActorRef;
    }

    public void setWhoToGreet(String whoToGreet) {
        this.greeting = message + ", " + whoToGreet;
    }

    public void greet() {
        String greetingMsg = greeting;
        printerActorRef.tell(printer -> printer.print(greetingMsg));
    }

}

