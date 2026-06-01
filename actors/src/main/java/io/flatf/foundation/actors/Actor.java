package io.flatf.foundation.actors;

import io.flatf.foundation.actors.impl.IActorSystem;

/**
 * Helper class to work with actor contexts
 */
public class Actor {

    private static final ThreadLocal<IActorRef<?>> currentActor = new ThreadLocal<>();

    private static final ThreadLocal<IActorRef<?>> callerActor = new ThreadLocal<>();

    private Actor() {
    }

    /**
     * Create a new actor system with the specified name.
     * <p>
     * Default ForkJoinScheduler is used.
     *
     * @param name actor system name
     * @return newly created actor system
     */
    public static io.flatf.foundation.actors.IActorSystem newSystem(String name) {
        return new IActorSystem(name);
    }

    /**
     * Create a new actor system with the specified name and scheduler factory.
     * <p>
     * Scheduler factory will be used to create actors; actor will own the scheduler, i.e. each scheduler is disposed together with its owning actor.
     *
     * @param name             actor system name
     * @param defaultScheduler default scheduler for new actors
     * @return newly created actor system
     */
    public static io.flatf.foundation.actors.IActorSystem newSystem(String name, IActorScheduler defaultScheduler) {
        return new IActorSystem(name, defaultScheduler);
    }

    /**
     * Gets the reference to the actor from its code.
     * <p>
     * When called from a properly called actor action, return this actor's ActorRef.
     * <p>
     * Returns null if called not from actor context
     *
     * @param <T> actor POJO class
     * @return {@link IActorRef} for the actor being called
     */
    @SuppressWarnings("unchecked")
    public static <T> IActorRef<T> current() {
        return (IActorRef<T>) currentActor.get();
    }

    /**
     * Gets the reference to the actor calling this actor.
     * <p>
     * When called from a properly called actor tell/ask/later action, return the actor from which context this actor's action was called.
     * <p>
     * If called in a callback for {@link IActorRef#ask} calls, this method returns a reference to the 'asked' actor.
     * <p>
     * For ask/later calls not from actor context, this method returns null
     *
     * @param <T> actor POJO class
     * @return {@link IActorRef} for the caller actor, or null if called not from actor context or from an actor called from outside any actor context
     */
    @SuppressWarnings("unchecked")
    public static <T> IActorRef<T> caller() {
        return (IActorRef<T>) callerActor.get();
    }

    /**
     * Returns the current actor's system.
     *
     * @return current actor system or null if called not from actor context
     */
    public static io.flatf.foundation.actors.IActorSystem system() {
        IActorRef<?> actor = current();
        return actor == null ? null : current().system();
    }

    /* Not a part of public API */
    public static void setCurrent(IActorRef<?> actor) {
        if (actor == null) {
            currentActor.remove();
        } else {
            currentActor.set(actor);
        }
    }

    /* Not a part of public API */
    public static void setCaller(IActorRef<?> actor) {
        if (actor == null) {
            callerActor.remove();
        } else {
            callerActor.set(actor);
        }
    }

}

