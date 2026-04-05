package com.coursework.rx;

/**
 * Обёртка над {@link Observer}: гарантирует не более одного терминального события
 * и не пропускает исключения из пользовательского кода в {@link Observer#onNext}.
 */
final class SerialEmitter<T> implements Emitter<T> {

    private final Observer<? super T> observer;
    private volatile boolean done;

    SerialEmitter(Observer<? super T> observer) {
        this.observer = observer;
    }

    @Override
    public void onNext(T item) {
        if (done) {
            return;
        }
        try {
            observer.onNext(item);
        } catch (Throwable t) {
            onError(t);
        }
    }

    @Override
    public void onError(Throwable t) {
        if (t == null) {
            t = new NullPointerException("onError called with null Throwable");
        }
        if (!tryTerminate()) {
            return;
        }
        observer.onError(t);
    }

    @Override
    public void onComplete() {
        if (!tryTerminate()) {
            return;
        }
        observer.onComplete();
    }

    private synchronized boolean tryTerminate() {
        if (done) {
            return false;
        }
        done = true;
        return true;
    }
}
