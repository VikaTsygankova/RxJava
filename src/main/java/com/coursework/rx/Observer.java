package com.coursework.rx;

/**
 * Наблюдатель потока событий: элементы, ошибка или нормальное завершение.
 */
public interface Observer<T> {

    void onNext(T item);

    void onError(Throwable t);

    void onComplete();
}
