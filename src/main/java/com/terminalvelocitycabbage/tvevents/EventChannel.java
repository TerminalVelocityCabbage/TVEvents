package com.terminalvelocitycabbage.tvevents;

import java.util.ArrayList;
import java.util.List;

public class EventChannel {

    private final String name;
    private final List<EventChannel> dependencies;

    EventChannel(String name) {
        this.name = name;
        this.dependencies = new ArrayList<>();
    }

    public String getName() {
        return name;
    }

    public EventChannel dependsOn(EventChannel other) {
        this.dependencies.add(other);
        return this;
    }

    public List<EventChannel> getDependencies() {
        return dependencies;
    }
}
