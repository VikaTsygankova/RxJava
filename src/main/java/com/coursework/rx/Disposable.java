package com.coursework.rx;

/**
 * Отмена подписки: дальнейшие события наблюдателю не доставляются (кооперативная отмена).
 */
public interface Disposable {

    void dispose();

    boolean isDisposed();
}
