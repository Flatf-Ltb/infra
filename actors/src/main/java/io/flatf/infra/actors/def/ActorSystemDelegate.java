package io.flatf.infra.actors.def;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import io.flatf.common.thread.ShutdownHooks;
import io.flatf.common.thread.Sleep;
import scala.concurrent.Future;

public final class ActorSystemDelegate {

    private final ActorSystem actorSystem;

    private final LoggingAdapter logger;

    public static ActorSystemDelegate newSystem(String name) {
        return new ActorSystemDelegate(name);
    }

    private ActorSystemDelegate(String name) {
        this.actorSystem = ActorSystem.create(name);
        this.logger = Logging.getLogger(actorSystem, this);
        // Add ShutdownHook
        ShutdownHooks.addShutdownHook("ActorSystemTerminateThread", this::terminateActorSystem);
    }

    private void terminateActorSystem() {
        logger.info("ActorSystem {} is terminated...", actorSystem.name());
        Future<Terminated> terminate = actorSystem.terminate();
        while (!terminate.isCompleted())
            Sleep.millis(100);
    }

    public ActorSystem get() {
        return actorSystem;
    }

    public String name() {
        return actorSystem.name();
    }

    public ActorRef actorOf(Props props) {
        return actorSystem.actorOf(props);
    }

    public ActorRef actorOf(Props props, String name) {
        return actorSystem.actorOf(props, name);
    }

    public ActorRef deadLetters() {
        return actorSystem.deadLetters();
    }

    public ActorSelection actorSelectionOf(String path) {
        return actorSystem.actorSelection(path);
    }

    public ActorSelection actorSelectionOf(ActorPath path) {
        return actorSystem.actorSelection(path);
    }

}

