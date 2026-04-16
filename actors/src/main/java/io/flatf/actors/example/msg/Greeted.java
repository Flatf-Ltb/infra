package io.flatf.actors.example.msg;

import akka.actor.typed.ActorRef;

import java.util.Objects;

/**
 * @author Akka official
 */
public record Greeted(
        String whom,
        ActorRef<Greet> from) {

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Greeted greeted = (Greeted) o;
        return Objects.equals(whom, greeted.whom) && Objects.equals(from, greeted.from);
    }

    @Override
    public String toString() {
        return "Greeted{" + "whom='" + whom + '\'' + ", from=" + from + '}';
    }

}

