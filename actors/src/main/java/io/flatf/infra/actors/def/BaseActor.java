package io.flatf.infra.actors.def;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.pf.ReceiveBuilder;

abstract class BaseActor extends AbstractActor {

    protected final ActorRef self = self();

    protected final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    protected BaseActor() {
        log.info("Created Actor -> {}", self);
    }

    protected final ReceiveBuilder getReceiveBuilder() {
        return receiveBuilder().matchAny(this::handleUnknown);
    }

    private void handleUnknown(Object obj) {
        log.error("Received unmatched message -> object type -> {}", obj.getClass().getName());
        handleUnknown0(obj);
    }

    protected abstract void handleUnknown0(Object t);

    protected void stop() {
        log.info("Stop actor -> {}", self);
        getContext().stop(self);
    }

}

