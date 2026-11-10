using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace KnightBus.Core.Outbox;

/// <summary>
/// The write side of the outbox, scoped to a DI scope. A store package implements it and provides
/// a way to enlist the caller's own database transaction, such as
/// <c>UseSqlTransaction</c> in KnightBus.SqlServer. Messages added to the session are written in
/// that transaction, so they are only dispatched if it commits.
/// </summary>
public interface IOutboxSession
{
    /// <summary>
    /// True when a transaction has been enlisted and is still open.
    /// </summary>
    bool IsEnlisted { get; }

    /// <summary>
    /// Writes the messages to the outbox in the enlisted transaction.
    /// </summary>
    /// <exception cref="OutboxNotEnlistedException">No open transaction is enlisted.</exception>
    Task AddAsync(IReadOnlyList<OutboxMessage> messages, CancellationToken cancellationToken);
}
