using System;

namespace KnightBus.Core.Outbox;

/// <summary>
/// An <see cref="OutboxMessage"/> locked by an <see cref="IOutboxStore"/> for dispatch.
/// </summary>
public sealed class OutboxRecord
{
    public OutboxRecord(OutboxMessage message, int attempts, Guid lockToken)
    {
        Message = message;
        Attempts = attempts;
        LockToken = lockToken;
    }

    public OutboxMessage Message { get; }

    /// <summary>
    /// How many times the message has been locked for dispatch, including the current attempt.
    /// </summary>
    public int Attempts { get; }

    /// <summary>
    /// Identifies the lock. The store only completes or releases the message while the lock is
    /// still the one it handed out, so a dispatcher whose lock expired cannot touch a message
    /// another dispatcher has taken over.
    /// </summary>
    public Guid LockToken { get; }
}
