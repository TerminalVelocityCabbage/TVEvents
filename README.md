# TVEvents

A deterministic, dependency-based event system for Java, designed for game engines and systems where event order matters.

## Goals

- **Determinism**: Ensure events are processed in a predictable order based on a directed acyclic graph (DAG) of channel dependencies.
- **Fluent API**: Provide a clean, readable interface for subscribing to and publishing events.
- **Async Support**: Allow listeners to run asynchronously while maintaining deterministic execution order between dependent channels.
- **Performance**: Optimize dispatching through internal caching of channel orders and subscription management.

## Installation

TVEvents is currently intended to be used as a git submodule in your project.

```bash
git submodule add https://github.com/YourUsername/TVEvents.git
```

Then, include it in your `settings.gradle`:

```gradle
include 'TVEvents'
```

And add it as a dependency in your `build.gradle`:

```gradle
dependencies {
    implementation project(':TVEvents')
}
```

## Core Concepts

### The Event Bus
The `EventBus` is the central hub for all event activity. Most applications will use a single global instance.

```java
EventBus eventBus = new EventBus();
```

### Event Channels & Dependencies
Channels group events and define the order in which they are dispatched. By defining dependencies, you can ensure that one channel's listeners always run before another's.

It is recommended to store your channels in a central location, such as static fields:

```java
public class MyGame {
    
    public static final EventBus BUS = new EventBus();

    public static class Channels {
        public static final EventChannel INPUT = BUS.createChannel("input");
        public static final EventChannel GAMEPLAY = BUS.createChannel("gameplay").dependsOn(INPUT);
        public static final EventChannel UI = BUS.createChannel("ui").dependsOn(GAMEPLAY);
    }
}
```
*In this example, any event published globally will hit `INPUT` listeners, then `GAMEPLAY`, then `UI`.*

## Basic Usage

### 1. Define an Event
Events are simple POJOs that implement the `Event` interface.

```java
public class PlayerJumpEvent implements Event {
    private final double power;

    public PlayerJumpEvent(double power) {
        this.power = power;
    }

    public double getPower() {
        return power;
    }
}
```

### 2. Subscribe to an Event
Use the fluent `subscribe` API to register listeners.

```java
MyGame.BUS.subscribe(PlayerJumpEvent.class)
    .onChannel(MyGame.Channels.GAMEPLAY)
    .handle(event -> {
        System.out.println("Player jumped with power: " + event.getPower());
    });
```

### 3. Publish an Event
Publishing also uses a fluent API. You can publish to the default global channel or a specific one.

```java
// Publishes to the global channel (and all downstream dependent channels)
MyGame.BUS.publish(new PlayerJumpEvent(10.0)).now();

// Publishes starting specifically at the GAMEPLAY channel
MyGame.BUS.publish(new PlayerJumpEvent(10.0)).to(MyGame.Channels.GAMEPLAY).now();
```

## Advanced Features

### Priority
Within a single channel, you can order listeners using priority. Higher numbers run first (default is 100).

```java
MyGame.BUS.subscribe(PlayerJumpEvent.class)
    .withPriority(200) // Runs before default priority listeners
    .handle(event -> { ... });
```

### Cancellation
Listeners can cancel an event to prevent it from reaching further listeners in the current or downstream channels.

```java
MyGame.BUS.subscribe(PlayerJumpEvent.class)
    .handle((event, status) -> {
        if (event.getPower() > 100) {
            status.cancel();
        }
    });

// When publishing, use .cancellable() to get the final status
Cancellable result = MyGame.BUS.publish(new PlayerJumpEvent(150)).cancellable();
if (result.isCancelled()) {
    // Event was cancelled by a listener
}
```

### Asynchronous Listeners
TVEvents supports asynchronous listeners that don't block the main dispatch thread but still respect channel dependencies. The bus will wait for async listeners in a channel to complete before moving to dependent channels.

```java
MyGame.BUS.subscribe(PlayerJumpEvent.class)
    .async()
    .handle(event -> {
        // This runs in a separate thread
        performHeavyCalculation(event);
    });
```

### Global Error Handling
Handle exceptions thrown by listeners gracefully without crashing the dispatch process.

```java
MyGame.BUS.setErrorHandler((subscription, event, throwable) -> {
    System.err.println("Error in listener: " + throwable.getMessage());
});
```