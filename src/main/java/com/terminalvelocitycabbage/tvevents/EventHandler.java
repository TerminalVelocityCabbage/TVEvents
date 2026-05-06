package com.terminalvelocitycabbage.tvevents;

@FunctionalInterface
public interface EventHandler {
    void handle(Subscription sub, Event event, Throwable throwable);
}
