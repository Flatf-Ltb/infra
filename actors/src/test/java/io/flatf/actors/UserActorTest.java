package io.flatf.actors;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.pattern.Patterns;
import io.flatf.actors.def.ActorSystemDelegate;
import io.flatf.common.thread.Sleep;
import scala.concurrent.Future;

public class UserActorTest {

    public static void main(String[] args) {

        ActorSystemDelegate actorSystem = ActorSystemDelegate.newSystem("data-center");

        ActorRef userActor1 = actorSystem.actorOf(UserActor.props(), "user1");
        ActorRef userActor2 = actorSystem.actorOf(UserActor.props(), "user2");
        ActorRef userActor3 = actorSystem.actorOf(UserActor.props(), "user3");
        ActorRef userActor4 = actorSystem.actorOf(UserActor.props(), "user4");

        userActor1.tell(new User(1, "user1", 10), ActorRef.noSender());
        userActor2.tell("dafaf", ActorRef.noSender());
        userActor3.tell(new User(1, "user1", 10), ActorRef.noSender());
        userActor4.tell(new User(1, "user1", 10), ActorRef.noSender());

        ActorSelection actorSelectionOf = actorSystem.actorSelectionOf("user");

        @SuppressWarnings("unused")
        Future<Object> ask = Patterns.ask(userActor1, 5, 1000);

        System.out.println(actorSelectionOf.pathString());

        Sleep.millis(3000);
        System.exit(0);

    }

}

