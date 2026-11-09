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
}
```

| Option | Default | Effect |
| --- | --- | --- |
| `EnablePartitioning` | `false` | Creates the entity as partitioned, for higher throughput. |
| `SupportOrdering` | `false` | Messages are forwarded to subscriptions in order. |
| `EnableBatchedOperations` | `true` | Server-side batching. |

Two caveats:

- **It is all or nothing.** A mapping that implements the interface supplies all three values; there
  is no per-property merge with the defaults.
- **It only applies at creation.** Changing these values does not reconfigure an entity that already
  exists — Azure requires most of them to be set when the entity is created.

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

## Retrying with a delay

A failed message is abandoned and Service Bus delivers it again right away: it has no way to abandon a
message with a delay. To wait between attempts, implement `IServiceBusRetryDelay` on the processing
settings. It is given the number of deliveries so far, the failed one included, so it can back off:

```csharp
public class BackoffSettings : IProcessingSettings, IServiceBusRetryDelay
{
    public int MaxConcurrentCalls => 10;
    public int PrefetchCount => 10;
    public TimeSpan MessageLockTimeout => TimeSpan.FromMinutes(5);
    public int DeadLetterDeliveryLimit => 5;

    // 10s, 20s, 40s, 80s
    public TimeSpan GetRetryDelay(int deliveryCount) =>
        TimeSpan.FromSeconds(10 * Math.Pow(2, deliveryCount - 1));
}
```

KnightBus schedules a copy of the failed message that far into the future and completes the
original. If the original cannot be completed, the copy is cancelled again and the original is
redelivered as usual; if the copy cannot be scheduled, the message is abandoned without a delay.

| On the copy | Why |
| --- | --- |
| `KnightBus.PreviousDeliveryCount` | Service Bus counts the deliveries of the copy from zero. KnightBus adds this to it, so `DeliveryCount` and `DeadLetterDeliveryLimit` count every attempt. |
| A new `MessageId` | So [duplicate detection](https://learn.microsoft.com/azure/service-bus-messaging/duplicate-detection) does not drop it. |
| `KnightBus.OriginalMessageId` | The id of the message as it was first sent. |
| `ErrorMessage`, `Exception` | The error of the latest attempt, as on an abandoned message. |

Some things to know:

- **Commands only.** A message cannot be sent to a single subscription, so failed events are
  redelivered right away; a warning is logged when an event's settings implement the interface.
- **Return `TimeSpan.Zero`** to redeliver right away.
- **No delay before dead-lettering.** The attempt that reaches `DeadLetterDeliveryLimit` is
  abandoned right away, since the next delivery only dead-letters it. Nor is there a delay when
  processing was cancelled, by a shutdown or a lost lock, rather than failed.
- **The queue's `MaxDeliveryCount` stops counting**, because every copy starts from zero, so
  `DeadLetterDeliveryLimit` is what limits the attempts.
- **A retry costs more than an abandon**: a scheduled send and a completion instead of one
  operation.
- **`MoveDeadLetters` resets the count.** The [management API](../features/management.md) drops
  `KnightBus.PreviousDeliveryCount` when it requeues dead letters, so they get every attempt again.
  Tools that resend a dead letter with its properties keep it; remove it there before resending.

## Dead letters

Service Bus provides a dead letter sub-queue per queue and per subscription. Read and requeue them
with the [management API](../features/management.md):

```csharp
services.AddServiceBusManagement(connectionString);
```

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
