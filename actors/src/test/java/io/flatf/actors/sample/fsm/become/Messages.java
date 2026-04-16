package io.flatf.actors.sample.fsm.become;

import akka.actor.ActorRef;

public class Messages {

	public record Busy(ActorRef chopstick) {
	}

	public record Put(ActorRef hakker) {
	}

	public record Take(ActorRef hakker) {
	}

	public record Taken(ActorRef chopstick) {
	}

	private interface EatMessage {
	}

	public static final Object Eat = new EatMessage() {
	};

	private interface ThinkMessage {
	}

	public static final Object Think = new ThinkMessage() {
	};
}

