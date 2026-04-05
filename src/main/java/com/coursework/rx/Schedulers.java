package com.coursework.rx;

/**
 * Фабрики планировщиков для курсовой реализации.
 */
public final class Schedulers {

    private static final IOThreadScheduler IO = new IOThreadScheduler("pj-io");
    private static final SingleThreadScheduler SINGLE = new SingleThreadScheduler("pj-single");
    private static final ComputationScheduler COMPUTATION = new ComputationScheduler("pj-computation");

    private Schedulers() {
    }

    public static Scheduler io() {
        return IO;
    }

    public static Scheduler single() {
        return SINGLE;
    }

    public static Scheduler computation() {
        return COMPUTATION;
    }
}
