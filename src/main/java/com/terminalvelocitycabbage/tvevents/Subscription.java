package com.terminalvelocitycabbage.tvevents;

import java.util.function.BiConsumer;

public record Subscription(EventBus bus, EventChannel channel, Class<? extends Event> eventClass, BiConsumer<Object, Cancellable> listener, int priority, boolean async) {
    public void unsubscribe() {
        bus.unsubscribe(this);
    }
}
