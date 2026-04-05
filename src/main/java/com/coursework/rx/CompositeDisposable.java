package com.coursework.rx;

import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Группа {@link Disposable}: {@link #dispose()} вызывает dispose у всех добавленных.
 */
public final class CompositeDisposable implements Disposable {

    private final CopyOnWriteArrayList<Disposable> disposables = new CopyOnWriteArrayList<>();
    private volatile boolean disposed;

    public void add(Disposable d) {
        if (d == null || disposed) {
            if (d != null) {
                d.dispose();
            }
            return;
        }
        disposables.add(d);
        if (disposed) {
            d.dispose();
            disposables.remove(d);
        }
    }

    @Override
    public void dispose() {
        disposed = true;
        for (Disposable d : disposables) {
            d.dispose();
        }
        disposables.clear();
    }

    @Override
    public boolean isDisposed() {
        return disposed;
    }
}
