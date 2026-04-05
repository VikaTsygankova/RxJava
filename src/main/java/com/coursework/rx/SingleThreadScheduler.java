package com.coursework.rx;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Аналог {@code Schedulers.single()}: один фоновый поток для всех задач этого экземпляра.
 */
public final class SingleThreadScheduler implements Scheduler {

    private final ExecutorService executor;

    public SingleThreadScheduler(String threadName) {
        Objects.requireNonNull(threadName, "threadName");
        ThreadFactory tf = runnable -> {
            Thread t = new Thread(runnable, threadName);
            t.setDaemon(true);
            return t;
        };
        this.executor = Executors.newSingleThreadExecutor(tf);
    }

    @Override
    public void execute(Runnable task) {
        executor.execute(task);
    }
}
