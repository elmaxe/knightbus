# Azure Service Bus

The most fully-featured KnightBus transport: queues, topics with subscriptions, native
dead-lettering, and deferred messages that can be cancelled.

```bash
dotnet add package KnightBus.Azure.ServiceBus
dotnet add package KnightBus.Azure.ServiceBus.Messages
```

## Registration

=== "Connection string"

    ```csharp
    services
        .UseServiceBus(config => config.ConnectionString = connectionString)
        .RegisterProcessors()
        .UseTransport<ServiceBusTransport>();
    ```

=== "Managed identity"

    ```csharp
    services
        .UseServiceBus(config =>
        {
            config.FullyQualifiedNamespace = "yournamespace.servicebus.windows.net";
            config.Credential = new ManagedIdentityCredential();
        })
        .RegisterProcessors()
        .UseTransport<ServiceBusTransport>();
    ```

    Leave `ConnectionString` unset when using a credential. Supplying neither a connection string nor
    a namespace-plus-credential throws at startup.

## Messages

| Interface | Kind |
| --- | --- |
| `IServiceBusCommand` | Command — one queue, one consumer |
| `IServiceBusEvent` | Event — one topic, many subscriptions |

```csharp
public class OrderPlaced : IServiceBusEvent
{
    public string OrderId { get; set; }
}

public class OrderPlacedMapping : IMessageMapping<OrderPlaced>
{
    public string QueueName => "order-placed";
}
```

Queues, topics and subscriptions are created automatically the first time they are needed.

## Subscriptions forward to a queue of their own

Each subscription forwards its messages to a queue named `{topic}.{subscription}`, and KnightBus
processes the subscription's messages from that queue. For the mapping above and a subscription named
`billing`, events arrive in the subscription `order-placed/billing` and are processed from the queue
`order-placed.billing`.

The point is dead letters. A message the handler gives up on is dead-lettered on the queue, and moving
it back puts it on that queue, so only the subscription that failed it handles it again. Without the
queue the only way back into a subscription is to publish to the topic again, which delivers the
message to every subscription — including the ones that already handled it.

The topic, the queue and the subscription are created together. A subscription that already exists,
for example one created by an earlier version of KnightBus, is pointed at its queue when the queue is
first created, so upgrading needs no manual step:

- Messages waiting on the subscription and new events are forwarded to the queue from then on. Dead
  letters already on the subscription stay there; moving them with the
  [management API](#dead-letters) sends them to the queue rather than to the topic.
- Service Bus does not let anything receive from a subscription that forwards. During a rolling
  upgrade, instances still on an older version stop receiving once the first upgraded instance
  switches the subscription, and the upgraded instances take over from the queue.
- Rolling back to an older version leaves the subscription forwarding, and the older version cannot
  receive from it. Remove `ForwardTo` from the subscription when you roll back.
- The queue is created with the queue defaults, like a command's queue: a `MaxDeliveryCount` of 10
  and a lock duration of one minute. Keep
  [`DeadLetterDeliveryLimit`](#processing-settings) below the queue's `MaxDeliveryCount`.

To keep receiving from the subscription itself, turn forwarding off for every topic, or for one
topic through its mapping's [creation options](#entity-creation-options):

```csharp
services.UseServiceBus(config =>
{
    config.ConnectionString = connectionString;
    config.DefaultCreationOptions.ForwardSubscriptionsToQueue = false;
});
```

Turning it off does not change a subscription that already forwards. Remove `ForwardTo` from the
subscription yourself, or the receiver waits on a subscription that never holds a message.

!!! note "Declaring entities up front"
    If your entities are declared rather than created by KnightBus — with infrastructure as code, or
    on the emulator, which KnightBus cannot create entities on — declare the queue
    `{topic}.{subscription}` and set the subscription's `ForwardTo` to it.
    `SubscriptionForwarding.QueueName(topic, subscription)` returns the name. The
    [Aspire host sample](https://github.com/BookBeat/knightbus/tree/master/samples/KnightBus.Samples.Azure.AspireHost)
    does this for the emulator.

## Client

```csharp
using var scope = host.Services.CreateScope();
var bus = scope.ServiceProvider.GetRequiredService<IServiceBus>();
```

| Method | Notes |
| --- | --- |
| `SendAsync<T>(T message)` | Send one command. |
| `SendAsync<T>(IEnumerable<T> messages)` | Batched send. |
| `PublishEventAsync<T>(T message)` | Publish one event. |
| `PublishEventsAsync<T>(IEnumerable<T> messages)` | Batched publish. |
| `ScheduleAsync<T>(T message, TimeSpan span)` | Deferred send. Returns a sequence number. |
| `ScheduleAsync<T>(IEnumerable<T> messages, TimeSpan span)` | Batched deferred send. Returns one sequence number per message, in input order. |
| `CancelScheduledAsync<T>(long sequenceNumber)` | Cancel a deferred message. |

!!! note "It is `PublishEventAsync`, not `PublishAsync`"
    Service Bus is the one transport whose publish methods are named after events. Every method takes
    an optional `CancellationToken`.

## Deferred messages

The delay is relative, and the returned sequence number is the only handle for cancelling:

```csharp
var sequenceNumber = await bus.ScheduleAsync(
    new SendReminder { UserId = userId },
    TimeSpan.FromHours(2)
);

await bus.CancelScheduledAsync<SendReminder>(sequenceNumber);
```

Batched scheduling is atomic — either all messages are scheduled or none are. Scheduling is available
for commands only; there is no deferred publish for events.

## Entity creation options

To control how KnightBus creates the queue or topic, implement `IServiceBusCreationOptions` on the
message's **mapping**:

```csharp
public class OrderPlacedMapping : IMessageMapping<OrderPlaced>, IServiceBusCreationOptions
{
    public string QueueName => "order-placed";

    public bool EnablePartitioning => true;
    public bool SupportOrdering => false;
    public bool EnableBatchedOperations => true;
    public bool ForwardSubscriptionsToQueue => true;
}
```

| Option | Default | Effect |
| --- | --- | --- |
| `EnablePartitioning` | `false` | Creates the entity as partitioned, for higher throughput. Also applies to a subscription's forwarding queue. |
| `SupportOrdering` | `false` | Messages are forwarded to subscriptions in order. |
| `EnableBatchedOperations` | `true` | Server-side batching. |
| `ForwardSubscriptionsToQueue` | `true` | Forwards each subscription of the topic to a [queue of its own](#subscriptions-forward-to-a-queue-of-their-own). |

Two caveats:

- **It is all or nothing.** A mapping that implements the interface supplies all of the values; there
  is no per-property merge with the defaults. `ForwardSubscriptionsToQueue` has a default
  implementation of `true` on the interface, so a mapping that does not declare it forwards even when
  `DefaultCreationOptions` turns forwarding off.
- **It only applies at creation.** Changing these values does not reconfigure an entity that already
  exists — Azure requires most of them to be set when the entity is created. The exception is
  `ForwardSubscriptionsToQueue`, which also points an existing subscription at its new queue.

To change the defaults for every message instead, adjust `DefaultCreationOptions` on the
configuration:

```csharp
services.UseServiceBus(config =>
{
    config.ConnectionString = connectionString;
    config.DefaultCreationOptions.EnablePartitioning = true;
});
```

The property itself is read-only on `IServiceBusConfiguration`, so mutate the existing options object
rather than assigning a new one. To assign one, build a concrete `ServiceBusConfiguration` and pass it
to the other overload:

```csharp
services.UseServiceBus(new ServiceBusConfiguration
{
    ConnectionString = connectionString,
    DefaultCreationOptions = new ServiceBusCreationOptions { EnablePartitioning = true },
});
```

## Processing settings

Settings map onto Service Bus' own processor options. Messages are received in peek-lock mode and
completed by KnightBus, and `MessageLockTimeout` becomes the maximum auto lock-renewal duration —
which means Service Bus renews the lock for you up to that limit. This is why
`ExtendMessageLockDurationMiddleware` is neither needed nor supported here.

`DeadLetterDeliveryLimit` must be **lower** than the queue's own `MaxDeliveryCount`, or Service Bus
dead-letters the message before KnightBus does and the
[`IProcessBeforeDeadLetter<T>`](../features/error-handling.md#hooking-into-dead-lettering) hook never
runs.

## Dead letters

Service Bus provides a dead letter sub-queue per queue and per subscription. Read and requeue them
with the [management API](../features/management.md):

```csharp
services.AddServiceBusManagement(connectionString);
```

An event's dead letters are on its subscription's forwarding queue, `{topic}.{subscription}`, which
the queue manager lists with the other queues. `MoveDeadLetters` on that queue redelivers them to that
subscription only. On the subscription manager:

- `MoveDeadLetters` moves the subscription's own dead letters to the entity it forwards to, and to the
  topic only when it does not forward.
- `Delete` also deletes the subscription's forwarding queue, with the messages on it. Otherwise a
  receiver would keep waiting on the queue and never create the subscription again.
- `Peek` and `Receive` throw on a subscription that forwards, since Service Bus does not let anything
  receive from it. Peek its queue instead.

This is also the only transport supporting `PeekScheduled` (inspecting deferred messages before
delivery) and `CancelScheduledMessage` through the management API.

## Attachments

Service Bus messages are size-limited, so large payloads belong in
[attachments](../features/attachments.md). This package ships no attachment provider, which is not a
gap — the provider is a host-wide choice, and either of the two that exist can back attachments here.
Blob Storage is the usual pick:

```csharp
services
    .UseServiceBus(config => config.ConnectionString = connectionString)
    .UseBlobStorage(storageConnectionString)
    .UseBlobStorageAttachments()
    .UseTransport<ServiceBusTransport>();
```

Note that only `UseTransport<ServiceBusTransport>()` appears — `UseBlobStorage(...)` supplies the
storage account for the attachments and does not start any Storage Queues listener. The same pattern
gives Service Bus messages [sagas](../features/sagas.md) and
[singleton processing](../features/singleton-processing.md), which likewise ship in other packages.

## Serialization

Defaults to `NewtonsoftSerializer`. See [serialization](../concepts/serialization.md) to change it.

## Example

[`KnightBus.Samples.Azure.ServiceBus`](https://github.com/BookBeat/knightbus/tree/master/samples/KnightBus.Samples.Azure.ServiceBus)
demonstrates commands, events with two subscriptions, the dead letter hook, creation options,
management and OpenTelemetry. A separate producer example and an Aspire host that runs both against
the Service Bus emulator sit alongside it.
