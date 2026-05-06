package com.terminalvelocitycabbage.tvevents;

import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

public class EventBusTest {

    static class TestEvent implements Event {
        String data;
        TestEvent(String data) { this.data = data; }
    }

    @Test
    void testBasicPublishSubscribe() {
        EventBus bus = new EventBus();
        List<String> received = new ArrayList<>();
        bus.subscribe(TestEvent.class).handle(e -> received.add(e.data));
        
        bus.publish(new TestEvent("hello")).now();
        assertEquals(1, received.size());
        assertEquals("hello", received.get(0));
    }

    @Test
    void testChannelDependencies() {
        EventBus bus = new EventBus();
        EventChannel input = bus.createChannel("input");
        EventChannel gameplay = bus.createChannel("gameplay").dependsOn(input);
        
        List<String> order = new ArrayList<>();
        bus.subscribe(TestEvent.class).onChannel(input).handle(e -> order.add("input"));
        bus.subscribe(TestEvent.class).onChannel(gameplay).handle(e -> order.add("gameplay"));
        bus.subscribe(TestEvent.class).handle(e -> order.add("global"));
        
        bus.publish(new TestEvent("test")).now();
        
        // Expected order: global (upstream of input), input, gameplay
        assertEquals(List.of("global", "input", "gameplay"), order);
    }

    @Test
    void testCancellation() {
        EventBus bus = new EventBus();
        EventChannel ch1 = bus.createChannel("ch1");
        EventChannel ch2 = bus.createChannel("ch2").dependsOn(ch1);
        
        List<String> order = new ArrayList<>();
        bus.subscribe(TestEvent.class).onChannel(ch1).handle((e, status) -> {
            order.add("ch1");
            status.cancel();
        });
        bus.subscribe(TestEvent.class).onChannel(ch2).handle(e -> order.add("ch2"));
        
        bus.publish(new TestEvent("test")).cancellable();
        
        assertEquals(List.of("ch1"), order);
        assertFalse(order.contains("ch2"));
    }

    @Test
    void testUnsubscribe() {
        EventBus bus = new EventBus();
        List<String> received = new ArrayList<>();
        Subscription sub = bus.subscribe(TestEvent.class).handle(e -> received.add(e.data));
        
        bus.publish(new TestEvent("first")).now();
        sub.unsubscribe();
        bus.publish(new TestEvent("second")).now();
        
        assertEquals(1, received.size());
        assertEquals("first", received.get(0));
    }
    
    @Test
    void testPublishToSpecificChannel() {
        EventBus bus = new EventBus();
        EventChannel input = bus.createChannel("input");
        EventChannel gameplay = bus.createChannel("gameplay").dependsOn(input);
        
        List<String> order = new ArrayList<>();
        bus.subscribe(TestEvent.class).handle(e -> order.add("global"));
        bus.subscribe(TestEvent.class).onChannel(input).handle(e -> order.add("input"));
        bus.subscribe(TestEvent.class).onChannel(gameplay).handle(e -> order.add("gameplay"));
        
        // Publishing to input should notify input and gameplay (downstream)
        // but NOT global (upstream)
        bus.publish(new TestEvent("test")).to(input).now();
        assertEquals(List.of("input", "gameplay"), order);
        
        order.clear();
        // Publishing to gameplay should notify ONLY gameplay
        // because nothing depends on it and input/global are upstream.
        bus.publish(new TestEvent("test2")).to(gameplay).now();
        assertEquals(List.of("gameplay"), order);
        
        order.clear();
        // Publishing to global should notify everything
        bus.publish(new TestEvent("test3")).now();
        assertEquals(List.of("global", "input", "gameplay"), order);
    }
    
    @Test
    void testBranchingDependencies() {
        EventBus bus = new EventBus();
        EventChannel a = bus.createChannel("A");
        EventChannel b = bus.createChannel("B");
        EventChannel c = bus.createChannel("C").dependsOn(a).dependsOn(b);
        
        List<String> order = new ArrayList<>();
        bus.subscribe(TestEvent.class).onChannel(a).handle(e -> order.add("A"));
        bus.subscribe(TestEvent.class).onChannel(b).handle(e -> order.add("B"));
        bus.subscribe(TestEvent.class).onChannel(c).handle(e -> order.add("C"));
        
        bus.publish(new TestEvent("test")).now();
        
        assertTrue(order.indexOf("A") < order.indexOf("C"));
        assertTrue(order.indexOf("B") < order.indexOf("C"));
        assertEquals(3, order.size()); 
    }
    
    @Test
    void testPriority() {
        EventBus bus = new EventBus();
        List<Integer> order = new ArrayList<>();
        bus.subscribe(TestEvent.class).withPriority(50).handle(e -> order.add(50));
        bus.subscribe(TestEvent.class).withPriority(150).handle(e -> order.add(150));
        bus.subscribe(TestEvent.class).withPriority(100).handle(e -> order.add(100));
        
        bus.publish(new TestEvent("test")).now();
        assertEquals(List.of(150, 100, 50), order);
    }

    @Test
    void testAsyncDeterministicOrder() throws Exception {
        EventBus bus = new EventBus();
        List<String> order = new ArrayList<>();
        
        bus.subscribe(TestEvent.class).async().handle(e -> {
            try { Thread.sleep(100); } catch (InterruptedException ex) {}
            order.add("async");
        });
        bus.subscribe(TestEvent.class).handle(e -> order.add("sync"));
        
        bus.publish(new TestEvent("test")).now().get();
        
        assertEquals(List.of("async", "sync"), order);
    }

    @Test
    void testGlobalErrorHandler() {
        EventBus bus = new EventBus();
        List<Throwable> errors = new ArrayList<>();
        bus.setErrorHandler((sub, event, t) -> errors.add(t));
        
        bus.subscribe(TestEvent.class).handle(e -> {
            throw new RuntimeException("Test Error");
        });
        
        bus.publish(new TestEvent("test")).now();
        assertEquals(1, errors.size());
        assertEquals("Test Error", errors.get(0).getMessage());
    }

    @Test
    void testRemoveChannel() {
        EventBus bus = new EventBus();
        EventChannel ch = bus.createChannel("toRemove");
        List<String> received = new ArrayList<>();
        bus.subscribe(TestEvent.class).onChannel(ch).handle(e -> received.add("received"));
        
        bus.publish(new TestEvent("test")).now();
        assertEquals(1, received.size());
        
        bus.removeChannel("toRemove");
        received.clear();
        bus.publish(new TestEvent("test2")).now();
        assertEquals(0, received.size());
    }

    @Test
    void testErrorPropagation() {
        EventBus bus = new EventBus();
        bus.subscribe(TestEvent.class).handle(e -> {
            throw new RuntimeException("Test Error");
        });
        
        List<String> nextReceived = new ArrayList<>();
        bus.subscribe(TestEvent.class).handle(e -> nextReceived.add("should not get here"));
        
        CompletableFuture<Void> future = bus.publish(new TestEvent("test")).now();
        ExecutionException ex = assertThrows(ExecutionException.class, future::get);
        assertEquals("Test Error", ex.getCause().getMessage());
        assertTrue(nextReceived.isEmpty());
    }

    @Test
    void testStopOnException() {
        EventBus bus = new EventBus();
        bus.setStopOnException(true);
        List<String> received = new ArrayList<>();
        
        bus.subscribe(TestEvent.class).withPriority(200).handle(e -> {
            received.add("first");
            throw new RuntimeException("Stop");
        });
        bus.subscribe(TestEvent.class).withPriority(100).handle(e -> received.add("second"));
        
        assertThrows(RuntimeException.class, () -> bus.publish(new TestEvent("test")).now().join());
        assertEquals(List.of("first"), received);
    }
}
