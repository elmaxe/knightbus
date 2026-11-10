using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace KnightBus.Core.Outbox;

/// <summary>
/// Sends outbox messages to a broker. Implemented by a transport package, such as
/// KnightBus.Azure.ServiceBus.
/// </summary>
public interface IOutboxTransport
{
    /// <summary>
    /// Sends messages that all have the same <see cref="OutboxMessage.Destination"/>.
    /// Throws if any of them could not be sent; the dispatcher then retries them, so some may be
    /// delivered more than once.
    /// </summary>
    Task SendAsync(IReadOnlyList<OutboxMessage> messages, CancellationToken cancellationToken);
}
