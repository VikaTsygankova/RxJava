package com.coursework.rx;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Аналог {@code Schedulers.computation()}: ограниченный пул размером с число доступных процессоров.
 */
public final class ComputationScheduler implements Scheduler {

    private final ExecutorService executor;

    public ComputationScheduler(String poolName) {
        this(poolName, defaultPoolSize());
    }

    public ComputationScheduler(String poolName, int poolSize) {
        Objects.requireNonNull(poolName, "poolName");
        int n = Math.max(1, poolSize);
        ThreadFactory tf = runnable -> {
            Thread t = new Thread(runnable, poolName + "-" + COUNTER.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
        this.executor = Executors.newFixedThreadPool(n, tf);
    }

    private static final AtomicInteger COUNTER = new AtomicInteger();

    private static int defaultPoolSize() {
        int cpus = Runtime.getRuntime().availableProcessors();
        return Math.max(1, cpus);
    }

    @Override
    public void execute(Runnable task) {
        executor.execute(task);
    }
}
