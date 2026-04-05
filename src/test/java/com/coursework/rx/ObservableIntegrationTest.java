package com.coursework.rx;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ObservableIntegrationTest {

    @Test
    void subscribeOn_runsCreateOnSchedulerThread() throws Exception {
        AtomicReference<String> subThread = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        Observable.<Integer>create(emitter -> {
            subThread.set(Thread.currentThread().getName());
            emitter.onNext(1);
            emitter.onComplete();
        })
                .subscribeOn(Schedulers.single())
                .subscribe(new Observer<Integer>() {
                    @Override
                    public void onNext(Integer item) {
                    }

                    @Override
                    public void onError(Throwable t) {
                        done.countDown();
                    }

                    @Override
                    public void onComplete() {
                        done.countDown();
                    }
                });
        assertTrue(done.await(5, TimeUnit.SECONDS));
        assertTrue(subThread.get() != null && subThread.get().startsWith("pj-single"));
    }

    @Test
    void observeOn_deliversEventsOnSchedulerThread() throws Exception {
        AtomicReference<String> observeThread = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        Observable.<Integer>create(emitter -> {
            emitter.onNext(7);
            emitter.onComplete();
        })
                .observeOn(Schedulers.single())
                .subscribe(new Observer<Integer>() {
                    @Override
                    public void onNext(Integer item) {
                        observeThread.set(Thread.currentThread().getName());
                    }

                    @Override
                    public void onError(Throwable t) {
                        done.countDown();
                    }

                    @Override
                    public void onComplete() {
                        done.countDown();
                    }
                });
        assertTrue(done.await(5, TimeUnit.SECONDS));
        assertTrue(observeThread.get() != null && observeThread.get().startsWith("pj-single"));
    }

    @Test
    void flatMap_mergesInnerStreams() {
        List<Integer> out = new ArrayList<>();
        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onNext(2);
            emitter.onComplete();
        })
                .flatMap(n -> Observable.<Integer>create(e -> {
                    e.onNext(n * 10);
                    e.onComplete();
                }))
                .subscribe(new Observer<Integer>() {
                    @Override
                    public void onNext(Integer item) {
                        out.add(item);
                    }

                    @Override
                    public void onError(Throwable t) {
                        throw new AssertionError(t);
                    }

                    @Override
                    public void onComplete() {
                    }
                });
        Collections.sort(out);
        assertEquals(Arrays.asList(10, 20), out);
    }

    @Test
    void disposable_isDisposed_afterComplete() {
        Disposable d = Observable.<String>create(emitter -> emitter.onComplete())
                .subscribe(new Observer<String>() {
                    @Override
                    public void onNext(String item) {
                    }

                    @Override
                    public void onError(Throwable t) {
                    }

                    @Override
                    public void onComplete() {
                    }
                });
        assertTrue(d.isDisposed());
    }

    @Test
    void subscribeOn_observeOn_chain_threadsDiffer() throws Exception {
        AtomicReference<String> sub = new AtomicReference<>();
        AtomicReference<String> obs = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        Observable.<Integer>create(emitter -> {
            sub.set(Thread.currentThread().getName());
            emitter.onNext(1);
            emitter.onComplete();
        })
                .subscribeOn(Schedulers.io())
                .observeOn(Schedulers.single())
                .subscribe(new Observer<Integer>() {
                    @Override
                    public void onNext(Integer item) {
                        obs.set(Thread.currentThread().getName());
                    }

                    @Override
                    public void onError(Throwable t) {
                        done.countDown();
                    }

                    @Override
                    public void onComplete() {
                        done.countDown();
                    }
                });
        assertTrue(done.await(5, TimeUnit.SECONDS));
        assertNotEquals(sub.get(), obs.get());
    }
}
