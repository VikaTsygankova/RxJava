package com.coursework.rx;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link Emitter} с {@link Disposable}: после {@link #dispose()} события в наблюдатель не идут.
 * Поддерживает {@link #attach(Disposable)} для каскадной отмены (например, внутренних подписок в {@code flatMap}).
 */
final class CancellableEmitter<T> implements Emitter<T>, Disposable {

    private final SerialEmitter<T> delegate;
    private final AtomicBoolean disposed = new AtomicBoolean();
    private final CompositeDisposable attached = new CompositeDisposable();

    CancellableEmitter(Observer<? super T> observer) {
        this.delegate = new SerialEmitter<>(observer);
    }

    void attach(Disposable d) {
        attached.add(d);
    }

    @Override
    public void onNext(T item) {
        if (disposed.get()) {
            return;
        }
        delegate.onNext(item);
    }

    @Override
    public void onError(Throwable t) {
        if (disposed.get()) {
            return;
        }
        delegate.onError(t);
        disposed.set(true);
    }

    @Override
    public void onComplete() {
        if (disposed.get()) {
            return;
        }
        delegate.onComplete();
        disposed.set(true);
    }

    @Override
    public void dispose() {
        if (!disposed.compareAndSet(false, true)) {
            return;
        }
        attached.dispose();
    }

    @Override
    public boolean isDisposed() {
        return disposed.get();
    }
}
