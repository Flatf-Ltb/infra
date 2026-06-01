package io.flatf.foundation.actors;

import io.flatf.foundation.actors.impl.BlockingThreadScheduler;
import io.flatf.foundation.actors.impl.ExecutorBasedScheduler;
import io.flatf.foundation.actors.impl.SingleThreadScheduler;
import io.flatf.foundation.actors.impl.ThreadPerActorScheduler;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;

/**
 * Static factory to create schedulers.
 */
public class Schedulers {

    private Schedulers() {
    }

    /**
     * Creates a scheduler based on the shared ForkJoinPool.
     *
     * @param throughput maximum number of pending actor messages to be processed at once
     * @return scheduler
     */
    public static IActorScheduler newForkJoinPoolScheduler(int throughput) {
        return new ExecutorBasedScheduler(ForkJoinPool.commonPool(), throughput);
    }

    /**
     * Creates a scheduler based on a user-provided ExecutorService.
     *
     * @param executorService executor service for scheduling the tasks
     * @param throughput      maximum number of pending actor messages to be processed at once
     * @return scheduler
     */
    public static IActorScheduler newExecutorBasedScheduler(ExecutorService executorService, int throughput) {
        return new ExecutorBasedScheduler(executorService, throughput);
    }

    /**
     * Creates a scheduler based on a thread pool with a fixed number of threads.
     *
     * @param threads    number of threads in the thread pool
     * @param throughput maximum number of pending actor messages to be processed at once
     * @return scheduler
     */
    public static IActorScheduler newFixedThreadPoolScheduler(int threads, int throughput) {
        return new ExecutorBasedScheduler(Executors.newFixedThreadPool(threads, runnable -> new Thread(runnable, "actor:fixed")), throughput);
    }

    /**
     * Creates a scheduler that processed all the actors messages sequentially in a single
     * user-supplied thread. See {@link BlockingThreadScheduler} for details.
     *
     * @return scheduler
     */
    public static BlockingThreadScheduler newBlockingThreadScheduler() {
        return new BlockingThreadScheduler();
    }

    /**
     * Creates a scheduler using a single thread for all actor calls.
     *
     * @return scheduler
     */
    public static IActorScheduler newSingleThreadScheduler() {
        return new SingleThreadScheduler();
    }

    /**
     * Creates a scheduler that creates a single-thread executor for each actor.
     *
     * @return scheduler
     */
    public static IActorScheduler newThreadPerActorScheduler() {
        return new ThreadPerActorScheduler();
    }

}

