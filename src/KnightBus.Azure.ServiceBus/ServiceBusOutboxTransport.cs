using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using KnightBus.Core.Outbox;

namespace KnightBus.Azure.ServiceBus;

/// <summary>
/// Sends outbox messages to Service Bus. The outbox message id becomes the
/// <see cref="ServiceBusMessage.MessageId"/>, so a queue or topic with duplicate detection drops a
/// message the dispatcher sent twice.
/// </summary>
public sealed class ServiceBusOutboxTransport : IOutboxTransport, IAsyncDisposable
{
    private readonly ServiceBusClient _client;
    private readonly Dictionary<string, ServiceBusSender> _senders = new(StringComparer.Ordinal);
    private readonly Lock _sendersLock = new();

    public ServiceBusOutboxTransport(IServiceBusConfiguration configuration)
        : this(ServiceBusClientFactory.CreateServiceBusClient(configuration)) { }

    internal ServiceBusOutboxTransport(ServiceBusClient client)
    {
        _client = client;
    }

    public async Task SendAsync(
        IReadOnlyList<OutboxMessage> messages,
        CancellationToken cancellationToken
    )
    {
        if (messages.Count == 0)
            return;

        var sender = GetSender(messages[0].Destination);
        var pending = new Queue<ServiceBusMessage>(messages.Count);
        foreach (var message in messages)
            pending.Enqueue(ToServiceBusMessage(message));

        while (pending.Count > 0)
        {
            using var batch = await sender
                .CreateMessageBatchAsync(cancellationToken)
                .ConfigureAwait(false);

            if (!batch.TryAddMessage(pending.Peek()))
                throw new ServiceBusMessageTooLargeException();
            pending.Dequeue();

            while (pending.Count > 0 && batch.TryAddMessage(pending.Peek()))
                pending.Dequeue();

            await sender.SendMessagesAsync(batch, cancellationToken).ConfigureAwait(false);
        }
    }

    internal static ServiceBusMessage ToServiceBusMessage(OutboxMessage message)
    {
        var serviceBusMessage = new ServiceBusMessage(message.Body)
        {
            MessageId = message.Id.ToString(),
            ContentType = message.ContentType,
        };
        foreach (var property in message.Properties)
            serviceBusMessage.ApplicationProperties[property.Key] = property.Value;
        if (message.ScheduledEnqueueTimeUtc is { } scheduled)
            serviceBusMessage.ScheduledEnqueueTime = scheduled;
        return serviceBusMessage;
    }

    private ServiceBusSender GetSender(string destination)
    {
        lock (_sendersLock)
        {
            if (!_senders.TryGetValue(destination, out var sender))
            {
                sender = _client.CreateSender(destination);
                _senders.Add(destination, sender);
            }
            return sender;
        }
    }

    public async ValueTask DisposeAsync()
    {
        List<ServiceBusSender> senders;
        lock (_sendersLock)
        {
            senders = [.. _senders.Values];
            _senders.Clear();
        }
        foreach (var sender in senders)
            await sender.DisposeAsync().ConfigureAwait(false);
        await _client.DisposeAsync().ConfigureAwait(false);
    }
}
