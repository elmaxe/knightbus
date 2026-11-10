# Transactional outbox

Saving to a database and then sending a message are two separate operations. If the process dies, or
the broker is unavailable, between the two, the database says one thing and the rest of the system
never hears about it. Send first and roll back, and the message announces something that never
happened.

The outbox removes the gap. Messages are written to a table in the **same database transaction** as
your own data, and a background dispatcher sends them to the broker once that transaction has
committed. A rolled back transaction leaves no message behind; a committed one is sent even if the
broker was down at the time.

The outbox is available for Azure Service Bus, with SQL Server as the store.

## Setting it up

```csharp
services
    .UseServiceBus(serviceBusConfiguration)
    .UseServiceBusOutbox()
    .UseSqlServerOutbox(sqlConnectionString);
```

`UseSqlServerOutbox` registers:

- a scoped `IOutboxSession`, which writes the messages in your transaction,
- the outbox table, created at startup if it does not exist,
- the dispatcher, a hosted service that sends the messages. It runs with or without the KnightBus
  host, so a producer-only web application can use the outbox as well.

## Writing messages

Inject `IServiceBusOutbox` alongside `IOutboxSession`, enlist your transaction in the session, and
use the outbox like `IServiceBus`:

```csharp
public class PlaceOrder(IOutboxSession outboxSession, IServiceBusOutbox outbox)
{
    public async Task Handle(Order order, CancellationToken ct)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync(ct);
        outboxSession.UseSqlTransaction(transaction);

        await InsertOrder(connection, transaction, order, ct);
        await outbox.PublishEventAsync(new OrderPlaced { OrderId = order.Id }, ct);

        await transaction.CommitAsync(ct);
    }
}
```

With Entity Framework Core, enlist the transaction of the `DbContext`:

```csharp
await using var transaction = await dbContext.Database.BeginTransactionAsync(ct);
outboxSession.UseSqlTransaction((SqlTransaction)transaction.GetDbTransaction());
```

Writing to the outbox without an open transaction throws `OutboxNotEnlistedException`. The message is
never sent or stored on its own, since that would quietly bring the gap back.

`IServiceBusOutbox` offers `SendAsync`, `ScheduleAsync`, `PublishEventAsync` and `PublishEventsAsync`,
with the same message types as `IServiceBus`. Two things differ:

- `ScheduleAsync` returns no sequence number, as the message only gets one once it is dispatched. A
  message scheduled through the outbox cannot be cancelled.
- Message pre-processors run when the message is written, not when it is sent. Distributed tracing
  therefore links the message to the operation that wrote it. Attachments are uploaded right away
  and are not part of the transaction: a rollback leaves the attachment in storage.

## Delivery guarantees

The dispatcher delivers each committed message **at least once**. A dispatcher that stops after
sending a message but before removing it from the table sends it again later. To make such repeats
harmless, either:

- make the receiving processors idempotent, or
- enable duplicate detection on the queue or topic. The dispatcher uses the outbox message id as the
  Service Bus `MessageId`, so the broker drops a repeat that arrives within the detection window.

Messages are dispatched oldest first. With several dispatchers, or after a retry, they can still
arrive out of order, as with messages sent directly.

## Several instances

Every instance can run a dispatcher against the same table. The dispatchers lock the rows they take
with `UPDLOCK, READPAST`, so they never wait on each other and never send the same message at the
same time. They also skip the rows of transactions that are still open instead of waiting for them.

To dispatch from some instances only, turn the dispatcher off in the others:

```csharp
services.UseSqlServerOutbox(sqlConnectionString, o => o.EnableDispatcher = false);
```

## Failures

A message that cannot be sent is retried with a doubling delay, from `MinRetryDelay` up to
`MaxRetryDelay`. When several messages to the same destination fail together, they are retried one
by one, so a single bad message, for example one that is too large, does not hold back the rest.

After `MaxAttempts` the message is marked as failed. It stays in the table with `FailedUtc` and
`LastError` set, and is not sent again. To retry it, clear `FailedUtc`.

## Settings

| Setting | Default | |
|---|---|---|
| `EnableDispatcher` | `true` | Whether this instance sends messages |
| `BatchSize` | 100 | Messages locked and sent at a time |
| `PollInterval` | 1 second | Wait before looking again once the outbox is empty; the longest a committed message waits before it is sent |
| `LockDuration` | 1 minute | How long a batch stays locked to one dispatcher. It must cover sending the batch |
| `MaxAttempts` | 10 | Attempts before a message is marked as failed |
| `MinRetryDelay` | 1 second | Wait before the first retry |
| `MaxRetryDelay` | 5 minutes | Longest wait between retries |
| `Schema` | `dbo` | Schema of the outbox table |
| `TableName` | `KnightBusOutbox` | Name of the outbox table |
| `CreateTable` | `true` | Create the schema and table at startup if they do not exist |

```csharp
services.UseSqlServerOutbox(
    sqlConnectionString,
    o =>
    {
        o.Schema = "messaging";
        o.PollInterval = TimeSpan.FromMilliseconds(500);
    }
);
```

## Creating the table yourself

To create the table with your own migrations, turn `CreateTable` off and take the script from
`SqlServerOutboxSchema.GetCreateScript(options)`. The script creates the schema and the table if
they do not exist, and is safe to run more than once.

## Other stores and transports

The outbox is built from three parts, so others can be added:

- `IOutboxSession` writes messages in the caller's transaction (store-specific),
- `IOutboxStore` locks, completes and retries messages for the dispatcher (store-specific),
- `IOutboxTransport` sends them to the broker (transport-specific).
