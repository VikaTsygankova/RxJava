package com.coursework.rx;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ObservableBasicTest {

    @Test
    void create_emitsItemsAndComplete() {
        List<Object> events = new ArrayList<>();
        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onNext(2);
            emitter.onComplete();
        }).subscribe(new Observer<Integer>() {
            @Override
            public void onNext(Integer item) {
                events.add("n" + item);
            }

            @Override
            public void onError(Throwable t) {
                events.add("e");
            }

            @Override
            public void onComplete() {
                events.add("c");
            }
        });
        assertEquals(Arrays.asList("n1", "n2", "c"), events);
    }

    @Test
    void create_propagatesErrorFromUpstream() {
        List<String> events = new ArrayList<>();
        RuntimeException boom = new RuntimeException("boom");
        Observable.<String>create(emitter -> {
            emitter.onNext("a");
            emitter.onError(boom);
        }).subscribe(new Observer<String>() {
            @Override
            public void onNext(String item) {
                events.add(item);
            }

            @Override
            public void onError(Throwable t) {
                events.add("err");
                assertEquals(boom, t);
            }

            @Override
            public void onComplete() {
                events.add("c");
            }
        });
        assertEquals(Arrays.asList("a", "err"), events);
    }

    @Test
    void create_swallowsEventsAfterTerminal() {
        List<Integer> nums = new ArrayList<>();
        Observable.<Integer>create(emitter -> {
            emitter.onComplete();
            emitter.onNext(42);
        }).subscribe(new Observer<Integer>() {
            @Override
            public void onNext(Integer item) {
                nums.add(item);
            }

            @Override
            public void onError(Throwable t) {
                throw new AssertionError(t);
            }

            @Override
            public void onComplete() {
            }
        });
        assertTrue(nums.isEmpty());
    }

    @Test
    void subscribe_handlesExceptionInSubscribeBody() {
        List<Object> events = new ArrayList<>();
        IllegalStateException ex = new IllegalStateException("fail");
        Observable.<Void>create(emitter -> {
            throw ex;
        }).subscribe(new Observer<Void>() {
            @Override
            public void onNext(Void item) {
            }

            @Override
            public void onError(Throwable t) {
                events.add(t);
            }

            @Override
            public void onComplete() {
                events.add("c");
            }
        });
        assertEquals(1, events.size());
        assertInstanceOf(IllegalStateException.class, events.get(0));
    }
}
