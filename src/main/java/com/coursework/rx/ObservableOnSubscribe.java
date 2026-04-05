package com.coursework.rx;

/**
 * Логика источника: вызывается при каждой подписке ({@link Observable#subscribe(Observer)}).
 */
@FunctionalInterface
public interface ObservableOnSubscribe<T> {

    void subscribe(Emitter<T> emitter);
}
