using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using KnightBus.Azure.ServiceBus.Messages;
using KnightBus.Core.Outbox;
using KnightBus.Core.PreProcessors;
using KnightBus.Messages;

namespace KnightBus.Azure.ServiceBus;

/// <summary>
/// Writes Service Bus messages to the outbox instead of sending them, in the transaction enlisted
/// in the scope's <see cref="IOutboxSession"/>. The <see cref="OutboxDispatcher"/> sends them once
/// that transaction has committed; nothing is sent if it rolls back.
/// </summary>
/// <remarks>
/// Unlike <see cref="IServiceBus"/>, scheduling does not return a sequence number, as the message
/// only gets one when it is dispatched. A message scheduled through the outbox cannot be cancelled.
/// </remarks>
public interface IServiceBusOutbox
{
    /// <summary>
    /// Writes a queue message to the outbox
    /// </summary>
    Task SendAsync<T>(T message, CancellationToken cancellationToken = default)
        where T : IServiceBusCommand;

    /// <summary>
    /// Writes queue messages to the outbox
    /// </summary>
    Task SendAsync<T>(IEnumerable<T> messages, CancellationToken cancellationToken = default)
        where T : IServiceBusCommand;

    /// <summary>
    /// Writes a queue message to the outbox, to be delivered <paramref name="delay"/> from now
    /// </summary>
    Task ScheduleAsync<T>(T message, TimeSpan delay, CancellationToken cancellationToken = default)
        where T : IServiceBusCommand;

    /// <summary>
    /// Writes queue messages to the outbox, to be delivered <paramref name="delay"/> from now
    /// </summary>
    Task ScheduleAsync<T>(
        IEnumerable<T> messages,
        TimeSpan delay,
        CancellationToken cancellationToken = default
    )
        where T : IServiceBusCommand;

    /// <summary>
    /// Writes a topic message to the outbox
    /// </summary>
    Task PublishEventAsync<T>(T message, CancellationToken cancellationToken = default)
        where T : IServiceBusEvent;

    /// <summary>
    /// Writes topic messages to the outbox
    /// </summary>
    Task PublishEventsAsync<T>(
        IEnumerable<T> messages,
        CancellationToken cancellationToken = default
    )
        where T : IServiceBusEvent;
}

public class ServiceBusOutbox : IServiceBusOutbox
{
    private readonly IOutboxSession _session;
    private readonly OutboxMessageFactory _factory;

    public ServiceBusOutbox(
        IOutboxSession session,
        IServiceBusConfiguration configuration,
        IEnumerable<IMessagePreProcessor> messagePreProcessors
    )
    {
        _session = session;
        _factory = new OutboxMessageFactory(configuration.MessageSerializer, messagePreProcessors);
    }

    public Task SendAsync<T>(T message, CancellationToken cancellationToken = default)
        where T : IServiceBusCommand => AddAsync([message], null, cancellationToken);

    public Task SendAsync<T>(IEnumerable<T> messages, CancellationToken cancellationToken = default)
        where T : IServiceBusCommand => AddAsync(messages, null, cancellationToken);

    public Task ScheduleAsync<T>(
        T message,
        TimeSpan delay,
        CancellationToken cancellationToken = default
    )
        where T : IServiceBusCommand =>
        AddAsync([message], DateTimeOffset.UtcNow.Add(delay), cancellationToken);

    public Task ScheduleAsync<T>(
        IEnumerable<T> messages,
        TimeSpan delay,
        CancellationToken cancellationToken = default
    )
        where T : IServiceBusCommand =>
        AddAsync(messages, DateTimeOffset.UtcNow.Add(delay), cancellationToken);

    public Task PublishEventAsync<T>(T message, CancellationToken cancellationToken = default)
        where T : IServiceBusEvent => AddAsync([message], null, cancellationToken);

    public Task PublishEventsAsync<T>(
        IEnumerable<T> messages,
        CancellationToken cancellationToken = default
    )
        where T : IServiceBusEvent => AddAsync(messages, null, cancellationToken);

    private async Task AddAsync<T>(
        IEnumerable<T> messages,
        DateTimeOffset? scheduledEnqueueTimeUtc,
        CancellationToken cancellationToken
    )
        where T : IMessage
    {
        //Fail before the pre-processors run, an attachment upload would otherwise be left behind
        if (!_session.IsEnlisted)
            throw new OutboxNotEnlistedException();

        var outboxMessages = new List<OutboxMessage>();
        foreach (var message in messages)
        {
            outboxMessages.Add(
                await _factory
                    .CreateAsync(message, scheduledEnqueueTimeUtc, cancellationToken)
                    .ConfigureAwait(false)
            );
        }

        if (outboxMessages.Count == 0)
            return;

        await _session.AddAsync(outboxMessages, cancellationToken).ConfigureAwait(false);
    }
}
