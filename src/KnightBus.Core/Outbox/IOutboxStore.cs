using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace KnightBus.Core.Outbox;

/// <summary>
/// The read side of a polling outbox store, used by the <see cref="OutboxDispatcher"/>.
/// Several dispatchers, in one process or many, may use the same store at once: a message locked
/// by one is not handed to another until its lock expires.
/// </summary>
public interface IOutboxStore
{
    /// <summary>
    /// Locks up to <paramref name="maxMessages"/> messages that are due for dispatch, oldest first,
    /// and counts the attempt. A message is due when it has not failed permanently and is neither
    /// locked nor waiting for a retry.
    /// </summary>
    Task<IReadOnlyList<OutboxRecord>> LockAsync(
        int maxMessages,
        TimeSpan lockDuration,
        CancellationToken cancellationToken
    );

    /// <summary>
    /// Removes messages that have been sent. Messages whose lock is no longer held are left alone.
    /// </summary>
    Task CompleteAsync(IReadOnlyList<OutboxRecord> records, CancellationToken cancellationToken);

    /// <summary>
    /// Releases the lock and makes the message due again after <paramref name="delay"/>.
    /// </summary>
    Task RetryLaterAsync(
        OutboxRecord record,
        Exception error,
        TimeSpan delay,
        CancellationToken cancellationToken
    );

    /// <summary>
    /// Marks the message as permanently failed. It stays in the store, but is not dispatched again.
    /// </summary>
    Task FailAsync(OutboxRecord record, Exception error, CancellationToken cancellationToken);
}
