using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using KnightBus.Core.PreProcessors;
using KnightBus.Messages;

namespace KnightBus.Core.Outbox;

/// <summary>
/// Turns a message into an <see cref="OutboxMessage"/> the same way a transport would before
/// sending it: the message's own serializer or the transport's default, and the registered
/// <see cref="IMessagePreProcessor"/>s.
/// </summary>
/// <remarks>
/// The pre-processors run when the message is written to the outbox, not when it is sent. Tracing
/// headers therefore link the message to the operation that wrote it. Attachments are uploaded
/// right away and are not part of the caller's transaction: a rollback leaves the attachment behind.
/// </remarks>
public class OutboxMessageFactory
{
    private readonly IMessageSerializer _defaultSerializer;
    private readonly IEnumerable<IMessagePreProcessor> _preProcessors;

    public OutboxMessageFactory(
        IMessageSerializer defaultSerializer,
        IEnumerable<IMessagePreProcessor> preProcessors
    )
    {
        _defaultSerializer = defaultSerializer;
        _preProcessors = preProcessors;
    }

    public async Task<OutboxMessage> CreateAsync<T>(
        T message,
        DateTimeOffset? scheduledEnqueueTimeUtc,
        CancellationToken cancellationToken
    )
        where T : IMessage
    {
        var serializer = AutoMessageMapper.GetMapping<T>() is ICustomMessageSerializer custom
            ? custom.MessageSerializer
            : _defaultSerializer;

        var properties = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var preProcessor in _preProcessors)
        {
            var preProcessed = await preProcessor
                .PreProcess(message, cancellationToken)
                .ConfigureAwait(false);
            foreach (var property in preProcessed)
            {
                var value =
                    property.Value as string
                    ?? Convert.ToString(property.Value, CultureInfo.InvariantCulture);
                if (value != null)
                    properties[property.Key] = value;
            }
        }

        return new OutboxMessage
        {
            //Version 7 ids grow over time, which keeps inserts at the end of an index on the id
            Id = Guid.CreateVersion7(),
            Destination = AutoMessageMapper.GetQueueName<T>(),
            Body = serializer.Serialize(message),
            ContentType = serializer.ContentType,
            Properties = properties,
            ScheduledEnqueueTimeUtc = scheduledEnqueueTimeUtc,
        };
    }
}
