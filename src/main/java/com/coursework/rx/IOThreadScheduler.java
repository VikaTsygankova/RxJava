package com.coursework.rx;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Аналог {@code Schedulers.io()}: неограниченно расширяемый пул потоков (как {@code CachedThreadPool}).
 */
public final class IOThreadScheduler implements Scheduler {

    private final ExecutorService executor;

    public IOThreadScheduler(String poolName) {
        Objects.requireNonNull(poolName, "poolName");
        ThreadFactory tf = runnable -> {
            Thread t = new Thread(runnable, poolName + "-" + COUNTER.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
        this.executor = Executors.newCachedThreadPool(tf);
    }

    private static final AtomicInteger COUNTER = new AtomicInteger();

    @Override
    public void execute(Runnable task) {
        executor.execute(task);
    }
}
