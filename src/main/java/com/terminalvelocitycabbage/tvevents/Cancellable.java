package com.terminalvelocitycabbage.tvevents;

public class Cancellable {

    private boolean cancelled;

    public boolean isCancelled() {
        return cancelled;
    }

    public void cancel() {
        this.cancelled = true;
    }

}
