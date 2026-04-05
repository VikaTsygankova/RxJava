package com.coursework.rx;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ObservableMapFilterTest {

    @Test
    void map_transformsEachItem() {
        List<Integer> out = new ArrayList<>();
        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onNext(2);
            emitter.onComplete();
        })
                .map(n -> n * 10)
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
        assertEquals(Arrays.asList(10, 20), out);
    }

    @Test
    void map_errorFromMapper_propagates() {
        List<String> events = new ArrayList<>();
        RuntimeException ex = new RuntimeException("map fail");
        Observable.<String>create(emitter -> {
            emitter.onNext("x");
            emitter.onComplete();
        })
                .map(s -> {
                    throw ex;
                })
                .subscribe(new Observer<Object>() {
                    @Override
                    public void onNext(Object item) {
                        events.add("n");
                    }

                    @Override
                    public void onError(Throwable t) {
                        events.add("e");
                        assertEquals(ex, t);
                    }

                    @Override
                    public void onComplete() {
                        events.add("c");
                    }
                });
        assertEquals(Arrays.asList("e"), events);
    }

    @Test
    void filter_skipsNonMatching() {
        List<Integer> out = new ArrayList<>();
        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onNext(2);
            emitter.onNext(3);
            emitter.onComplete();
        })
                .filter(n -> n % 2 == 0)
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
        assertEquals(Arrays.asList(2), out);
    }

    @Test
    void map_then_filter_chain() {
        List<String> out = new ArrayList<>();
        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onNext(2);
            emitter.onNext(3);
            emitter.onComplete();
        })
                .map(n -> "v" + n)
                .filter(s -> s.endsWith("2") || s.endsWith("3"))
                .subscribe(new Observer<String>() {
                    @Override
                    public void onNext(String item) {
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
        assertEquals(Arrays.asList("v2", "v3"), out);
    }
}
