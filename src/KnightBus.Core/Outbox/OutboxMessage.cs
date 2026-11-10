using System;
using System.Collections.Generic;

namespace KnightBus.Core.Outbox;

/// <summary>
/// A message that has been serialized and stored in the outbox, waiting to be sent by the
/// <see cref="OutboxDispatcher"/>. It holds everything the transport needs, so sending it does not
/// depend on the message type or the serializer.
/// </summary>
public sealed class OutboxMessage
{
    private static readonly IReadOnlyDictionary<string, string> NoProperties =
        new Dictionary<string, string>();

    /// <summary>
    /// Unique id of the message. Transports use it as the message id, so a broker with duplicate
    /// detection can drop a message the dispatcher sent twice.
    /// </summary>
    public required Guid Id { get; init; }

    /// <summary>
    /// The queue or topic the message is sent to.
    /// </summary>
    public required string Destination { get; init; }

    /// <summary>
    /// The serialized message.
    /// </summary>
    public required byte[] Body { get; init; }

    /// <summary>
    /// Content type of <see cref="Body"/>, from the serializer.
    /// </summary>
    public string? ContentType { get; init; }

    /// <summary>
    /// Message properties, such as distributed tracing headers, captured when the message was
    /// written to the outbox.
    /// </summary>
    public IReadOnlyDictionary<string, string> Properties { get; init; } = NoProperties;

    /// <summary>
    /// When set, the message is not delivered to receivers before this time.
    /// </summary>
    public DateTimeOffset? ScheduledEnqueueTimeUtc { get; init; }
}
