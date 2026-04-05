package com.coursework.rx;

/**
 * Планировщик: выполняет задачи асинхронно (аналог {@code rx.Scheduler} / {@code Worker} в RxJava 2).
 */
public interface Scheduler {

    void execute(Runnable task);
}
