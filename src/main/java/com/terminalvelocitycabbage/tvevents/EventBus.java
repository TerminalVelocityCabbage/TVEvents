package com.terminalvelocitycabbage.tvevents;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class EventBus {

    private final Map<String, EventChannel> channels = new ConcurrentHashMap<>();
    private final Map<EventChannel, Map<Class<? extends Event>, List<Subscription>>> subscriptions = new ConcurrentHashMap<>();
    private final Map<EventChannel, List<EventChannel>> dispatchOrderCache = new ConcurrentHashMap<>();
    private final EventChannel globalChannel;
    private EventHandler errorHandler;
    private boolean stopOnException = false;

    public EventBus() {
        globalChannel = new EventChannel("global");
        channels.put(globalChannel.getName(), globalChannel);
    }

    public void setStopOnException(boolean stopOnException) {
        this.stopOnException = stopOnException;
    }

    public PublishBuilder publish(Event event) {
        return new PublishBuilder(this, event);
    }

    private CompletableFuture<Void> publishTo(EventChannel channel, Event event) {
        try {
            List<EventChannel> order = getDispatchOrder(channel);
            CompletableFuture<Void> future = null;
            for (EventChannel target : order) {
                CompletableFuture<Void> channelFuture = notifyChannel(target, event, null);
                if (channelFuture != null) {
                    if (future == null) future = channelFuture;
                    else future = future.thenCompose(v -> channelFuture);
                }
            }
            return future != null ? future : CompletableFuture.completedFuture(null);
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
    }

    private Cancellable publishCancellableTo(EventChannel channel, Event event) {
        Cancellable status = new Cancellable();
        List<EventChannel> order = getDispatchOrder(channel);
        for (EventChannel target : order) {
            if (status.isCancelled()) break;
            CompletableFuture<Void> channelFuture = notifyChannel(target, event, status);
            // Note: For simplicity, publishCancellable is synchronous regarding the chain start,
            // but async listeners will still run in their threads.
            // If strict ordering with async is needed for cancellable, it would require returning a CF.
            if (channelFuture != null && !channelFuture.isDone()) {
                channelFuture.join(); // Wait to ensure cancellation is processed before next channel
            }
        }
        return status;
    }

    private List<EventChannel> getDispatchOrder(EventChannel channel) {
        return dispatchOrderCache.computeIfAbsent(channel, this::calculateDispatchOrder);
    }

    private List<EventChannel> calculateDispatchOrder(EventChannel startChannel) {
        List<EventChannel> order = new ArrayList<>();
        Set<EventChannel> visited = new HashSet<>();
        
        markUpstreamVisited(startChannel, visited);
        
        order.add(startChannel);
        visited.add(startChannel);
        
        boolean added;
        do {
            added = false;
            List<EventChannel> layer = new ArrayList<>();
            for (EventChannel candidate : channels.values()) {
                if (visited.contains(candidate)) continue;
                
                for (EventChannel dep : candidate.getDependencies()) {
                    if (visited.contains(dep)) {
                        boolean allDepsVisited = true;
                        for (EventChannel candidateDep : candidate.getDependencies()) {
                            if (!visited.contains(candidateDep)) {
                                allDepsVisited = false;
                                break;
                            }
                        }
                        
                        if (allDepsVisited) {
                            layer.add(candidate);
                        }
                        break;
                    }
                }
            }
            if (!layer.isEmpty()) {
                layer.sort(Comparator.comparing(EventChannel::getName));
                order.addAll(layer);
                visited.addAll(layer);
                added = true;
            }
        } while (added);
        
        return Collections.unmodifiableList(order);
    }

    private void markUpstreamVisited(EventChannel channel, Set<EventChannel> visited) {
        for (EventChannel dependency : channel.getDependencies()) {
            if (visited.add(dependency)) {
                markUpstreamVisited(dependency, visited);
            }
        }
    }

    private CompletableFuture<Void> notifyChannel(EventChannel channel, Event event, Cancellable status) {
        Map<Class<? extends Event>, List<Subscription>> channelSubs = subscriptions.get(channel);
        if (channelSubs == null) return null;

        List<Subscription> subs = channelSubs.get(event.getClass());
        if (subs == null || subs.isEmpty()) return null;

        CompletableFuture<Void> future = null;
        for (Subscription sub : subs) {
            if (status != null && status.isCancelled()) break;

            if (sub.async()) {
                CompletableFuture<Void> asyncSubFuture = CompletableFuture.runAsync(() -> {
                    try {
                        sub.listener().accept(event, status);
                    } catch (Throwable t) {
                        handleException(sub, event, t);
                    }
                });
                if (future == null) future = asyncSubFuture;
                else future = future.thenCompose(v -> asyncSubFuture);
            } else {
                if (future == null) {
                    try {
                        sub.listener().accept(event, status);
                    } catch (Throwable t) {
                        handleException(sub, event, t);
                        if (stopOnException) {
                            if (status != null) status.cancel();
                            break;
                        }
                    }
                } else {
                    future = future.thenRun(() -> {
                        if (status != null && status.isCancelled()) return;
                        try {
                            sub.listener().accept(event, status);
                        } catch (Throwable t) {
                            handleException(sub, event, t);
                        }
                    });
                }
            }
        }
        return future;
    }

    private void handleException(Subscription sub, Event event, Throwable t) {
        if (errorHandler != null) {
            errorHandler.handle(sub, event, t);
        }
        if (stopOnException || errorHandler == null) {
            if (t instanceof RuntimeException re) throw re;
            if (t instanceof Error e) throw e;
            throw new RuntimeException(t);
        }
    }

    public void setErrorHandler(EventHandler errorHandler) {
        this.errorHandler = errorHandler;
    }

    public <T extends Event> SubscriptionBuilder<T> subscribe(Class<T> eventClass) {
        return new SubscriptionBuilder<>(this, eventClass);
    }

    public Subscription subscribeTo(EventChannel channel, Class<? extends Event> eventClass, int priority, boolean async, BiConsumer<Object, Cancellable> listener) {
        getDispatchOrder(channel); // Ensure cache is populated
        Subscription sub = new Subscription(this, channel, eventClass, listener, priority, async);
        List<Subscription> subs = subscriptions.computeIfAbsent(channel, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(eventClass, k -> new CopyOnWriteArrayList<>());
        
        synchronized (subs) {
            subs.add(sub);
            subs.sort(Comparator.comparingInt(Subscription::priority).reversed());
        }
        return sub;
    }

    public void unsubscribe(Subscription subscription) {
        Map<Class<? extends Event>, List<Subscription>> channelSubs = subscriptions.get(subscription.channel());
        if (channelSubs != null) {
            List<Subscription> subs = channelSubs.get(subscription.eventClass());
            if (subs != null) {
                subs.remove(subscription);
            }
        }
    }

    public void removeChannel(String name) {
        EventChannel channel = channels.remove(name);
        if (channel != null) {
            subscriptions.remove(channel);
            dispatchOrderCache.entrySet().removeIf(entry -> 
                entry.getKey().equals(channel) || entry.getValue().contains(channel)
            );
        }
    }

    public EventChannel createChannel(String name) {
        return channels.computeIfAbsent(name, k -> {
            dispatchOrderCache.clear();
            EventChannel channel = new EventChannel(k);
            channel.dependsOn(globalChannel);
            return channel;
        });
    }

    public EventChannel getChannel(String name) {
        return channels.get(name);
    }

    public static class SubscriptionBuilder<T extends Event> {
        private final EventBus bus;
        private final Class<T> eventClass;
        private EventChannel channel;
        private int priority = 100;
        private boolean async = false;

        private SubscriptionBuilder(EventBus bus, Class<T> eventClass) {
            this.bus = bus;
            this.eventClass = eventClass;
            this.channel = bus.globalChannel;
        }

        public SubscriptionBuilder<T> onChannel(EventChannel channel) {
            this.channel = channel;
            return this;
        }

        public SubscriptionBuilder<T> withPriority(int priority) {
            this.priority = priority;
            return this;
        }

        public SubscriptionBuilder<T> async() {
            this.async = true;
            return this;
        }

        public Subscription handle(Consumer<T> listener) {
            return register((event, context) -> listener.accept((T) event));
        }

        public Subscription handle(BiConsumer<T, Cancellable> listener) {
            return register((event, context) -> listener.accept((T) event, context));
        }

        private Subscription register(BiConsumer<Object, Cancellable> listener) {
            return bus.subscribeTo(channel, eventClass, priority, async, listener);
        }
    }

    public static class PublishBuilder {
        private final EventBus bus;
        private final Event event;
        private EventChannel channel;

        private PublishBuilder(EventBus bus, Event event) {
            this.bus = bus;
            this.event = event;
            this.channel = bus.globalChannel;
        }

        public PublishBuilder to(EventChannel channel) {
            this.channel = channel;
            return this;
        }

        public CompletableFuture<Void> now() {
            return bus.publishTo(channel, event);
        }

        public Cancellable cancellable() {
            return bus.publishCancellableTo(channel, event);
        }
    }
}
