package com.coursework.rx;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.Objects;

/**
 * Холодный observable: логика из {@link #create(ObservableOnSubscribe)} выполняется при каждой подписке.
 */
public final class Observable<T> {

    private final ObservableOnSubscribe<T> onSubscribe;

    private Observable(ObservableOnSubscribe<T> onSubscribe) {
        this.onSubscribe = onSubscribe;
    }

    /**
     * Создаёт поток; фабрика {@code source} подписывается на выдачу через {@link Emitter}.
     */
    public static <T> Observable<T> create(ObservableOnSubscribe<T> source) {
        return new Observable<>(Objects.requireNonNull(source, "source"));
    }

    /**
     * Активирует источник. Возвращает {@link Disposable} для отмены (кооперативно).
     */
    public Disposable subscribe(Observer<? super T> observer) {
        Objects.requireNonNull(observer, "observer");
        CancellableEmitter<T> emitter = new CancellableEmitter<>(observer);
        try {
            onSubscribe.subscribe(emitter);
        } catch (Throwable t) {
            emitter.onError(t);
        }
        return emitter;
    }

    /**
     * Подписка на upstream выполняется в потоке {@code scheduler} (тело {@link ObservableOnSubscribe#subscribe}).
     */
    public Observable<T> subscribeOn(Scheduler scheduler) {
        Objects.requireNonNull(scheduler, "scheduler");
        final Observable<T> upstream = this;
        return create(emitter -> scheduler.execute(() -> {
            try {
                upstream.onSubscribe.subscribe(emitter);
            } catch (Throwable t) {
                emitter.onError(t);
            }
        }));
    }

    /**
     * Вызовы {@code onNext} / {@code onError} / {@code onComplete} к наблюдателю выполняются через {@code scheduler}
     * (очередь сохраняет порядок событий для данной подписки).
     */
    public Observable<T> observeOn(Scheduler scheduler) {
        Objects.requireNonNull(scheduler, "scheduler");
        final Observable<T> upstream = this;
        return create(emitter -> {
            BlockingQueue<ObserveEvent<T>> queue = new LinkedBlockingQueue<>();
            scheduler.execute(() -> {
                try {
                    while (true) {
                        ObserveEvent<T> e = queue.take();
                        switch (e.kind) {
                            case NEXT:
                                emitter.onNext(e.value);
                                break;
                            case ERROR:
                                emitter.onError(e.error);
                                return;
                            case COMPLETE:
                                emitter.onComplete();
                                return;
                            default:
                                throw new IllegalStateException(String.valueOf(e.kind));
                        }
                    }
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    emitter.onError(ex);
                }
            });
            upstream.onSubscribe.subscribe(new SerialEmitter<>(new Observer<T>() {
                @Override
                public void onNext(T item) {
                    try {
                        queue.put(ObserveEvent.next(item));
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        emitter.onError(ex);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    try {
                        queue.put(ObserveEvent.error(t));
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                }

                @Override
                public void onComplete() {
                    try {
                        queue.put(ObserveEvent.complete());
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                }
            }));
        });
    }

    /**
     * Преобразует каждый элемент с помощью {@code mapper}. Исключение из {@code mapper.apply}
     * направляется в {@link Observer#onError(Throwable)}.
     */
    public <R> Observable<R> map(Function<? super T, ? extends R> mapper) {
        Objects.requireNonNull(mapper, "mapper");
        final Observable<T> upstream = this;
        return create(emitter -> upstream.onSubscribe.subscribe(new SerialEmitter<>(new Observer<T>() {
            @Override
            public void onNext(T item) {
                R out;
                try {
                    out = mapper.apply(item);
                } catch (Throwable t) {
                    emitter.onError(t);
                    return;
                }
                emitter.onNext(out);
            }

            @Override
            public void onError(Throwable t) {
                emitter.onError(t);
            }

            @Override
            public void onComplete() {
                emitter.onComplete();
            }
        })));
    }

    /**
     * Пропускает только элементы, для которых {@code predicate.test} вернул true.
     */
    public Observable<T> filter(Predicate<? super T> predicate) {
        Objects.requireNonNull(predicate, "predicate");
        final Observable<T> upstream = this;
        return create(emitter -> upstream.onSubscribe.subscribe(new SerialEmitter<>(new Observer<T>() {
            @Override
            public void onNext(T item) {
                boolean pass;
                try {
                    pass = predicate.test(item);
                } catch (Throwable t) {
                    emitter.onError(t);
                    return;
                }
                if (pass) {
                    emitter.onNext(item);
                }
            }

            @Override
            public void onError(Throwable t) {
                emitter.onError(t);
            }

            @Override
            public void onComplete() {
                emitter.onComplete();
            }
        })));
    }

    /**
     * Для каждого элемента строит внутренний поток и объединяет его элементы в выходной поток (merge).
     */
    public <R> Observable<R> flatMap(Function<? super T, ? extends Observable<? extends R>> mapper) {
        Objects.requireNonNull(mapper, "mapper");
        final Observable<T> upstream = this;
        return create(emitter -> {
            SynchronizedEmitter<R> out = new SynchronizedEmitter<>(emitter);
            AtomicInteger wip = new AtomicInteger(1);
            AtomicBoolean stopped = new AtomicBoolean();

            Runnable endInner = () -> {
                if (wip.decrementAndGet() == 0 && !stopped.get()) {
                    out.onComplete();
                }
            };

            upstream.onSubscribe.subscribe(new SerialEmitter<>(new Observer<T>() {
                @Override
                public void onNext(T item) {
                    if (stopped.get()) {
                        return;
                    }
                    Observable<? extends R> inner;
                    try {
                        inner = mapper.apply(item);
                    } catch (Throwable t) {
                        stopped.set(true);
                        out.onError(t);
                        return;
                    }
                    wip.incrementAndGet();
                    Disposable sub = Objects.requireNonNull(inner, "inner Observable").subscribe(new Observer<R>() {
                        @Override
                        public void onNext(R value) {
                            if (!stopped.get()) {
                                out.onNext(value);
                            }
                        }

                        @Override
                        public void onError(Throwable t) {
                            stopped.set(true);
                            endInner.run();
                            out.onError(t);
                        }

                        @Override
                        public void onComplete() {
                            endInner.run();
                        }
                    });
                    attachIfPossible(emitter, sub);
                }

                @Override
                public void onError(Throwable t) {
                    if (!stopped.compareAndSet(false, true)) {
                        return;
                    }
                    out.onError(t);
                }

                @Override
                public void onComplete() {
                    if (stopped.get()) {
                        return;
                    }
                    endInner.run();
                }
            }));
        });
    }

    private static void attachIfPossible(Emitter<?> emitter, Disposable sub) {
        if (emitter instanceof CancellableEmitter) {
            ((CancellableEmitter<?>) emitter).attach(sub);
        }
    }
}
