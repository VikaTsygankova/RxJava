package com.coursework.rx;

/**
 * Сообщение для очереди в {@link Observable#observeOn(Scheduler)}.
 */
final class ObserveEvent<T> {

    enum Kind {
        NEXT, ERROR, COMPLETE
    }

    final Kind kind;
    final T value;
    final Throwable error;

    private ObserveEvent(Kind kind, T value, Throwable error) {
        this.kind = kind;
        this.value = value;
        this.error = error;
    }

    static <T> ObserveEvent<T> next(T value) {
        return new ObserveEvent<>(Kind.NEXT, value, null);
    }

    static <T> ObserveEvent<T> error(Throwable t) {
        return new ObserveEvent<>(Kind.ERROR, null, t);
    }

    static <T> ObserveEvent<T> complete() {
        return new ObserveEvent<>(Kind.COMPLETE, null, null);
    }
}
