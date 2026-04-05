package com.coursework.rx;

/**
 * Точка выдачи событий из {@link Observable#create(ObservableOnSubscribe)}.
 * После {@link #onError(Throwable)} или {@link #onComplete()} дальнейшие {@link #onNext} игнорируются.
 */
public interface Emitter<T> {

    void onNext(T item);

    void onError(Throwable t);

    void onComplete();
}
