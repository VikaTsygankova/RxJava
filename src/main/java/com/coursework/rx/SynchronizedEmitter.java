package com.coursework.rx;

/**
 * Сериализует вызовы к нижестоящему {@link Emitter} (для merge из нескольких потоков в {@code flatMap}).
 */
final class SynchronizedEmitter<T> implements Emitter<T> {

    private final Emitter<T> delegate;
    private final Object lock = new Object();

    SynchronizedEmitter(Emitter<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void onNext(T item) {
        synchronized (lock) {
            delegate.onNext(item);
        }
    }

    @Override
    public void onError(Throwable t) {
        synchronized (lock) {
            delegate.onError(t);
        }
    }

    @Override
    public void onComplete() {
        synchronized (lock) {
            delegate.onComplete();
        }
    }
}
