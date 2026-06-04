package io.flatf.infra.actors.impl;

import io.flatf.infra.actors.IActorScheduler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Scheduler that creates a single-thread executor for each actor.
 */
public class ThreadPerActorScheduler implements IActorScheduler {

    private final Map<Object, ExecutorService> executors = new ConcurrentHashMap<>();

    @Override
    public void actorCreated(Object actorId) {
        executors.put(actorId, Executors.newSingleThreadExecutor(runnable -> new Thread(runnable, "actor:" + actorId)));
    }

    @Override
    public void actorDisposed(Object actorId) {
        ExecutorService service = executors.remove(actorId);
        service.shutdown();
    }

    @Override
    public void schedule(Runnable task, Object actorId) {
        ExecutorService executor = executors.get(actorId);
        if (!executor.isShutdown()) {
            executor.execute(task);
        }
    }

    @Override
    public void close() {
        executors.values().forEach(ExecutorService::shutdown);
    }
}

