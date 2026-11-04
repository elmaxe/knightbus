using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using KnightBus.Core.Management;
using QueueProperties = KnightBus.Core.Management.QueueProperties;

namespace KnightBus.Azure.ServiceBus.Management;

public class ServiceBusSubscriptionManager : IQueueManager
{
    private readonly string _topic;
    private readonly ServiceBusAdministrationClient _adminClient;
    private readonly ServiceBusClient _client;

    public ServiceBusSubscriptionManager(
        string topic,
        ServiceBusClient client,
        ServiceBusAdministrationClient adminClient
    )
    {
        _topic = topic;
        _adminClient = adminClient;
        _client = client;
    }

    public async Task<IEnumerable<QueueProperties>> List(CancellationToken ct)
    {
        var subs = _adminClient.GetSubscriptionsRuntimePropertiesAsync(_topic, ct);
        var properties = new List<QueueProperties>();
        await foreach (var sub in subs)
            properties.Add(sub.ToQueueProperties(this, _topic));

        return properties;
    }

    public async Task<QueueProperties> Get(string path, CancellationToken ct)
    {
        var sub = await _adminClient
            .GetSubscriptionRuntimePropertiesAsync(_topic, path, ct)
            .ConfigureAwait(false);
        return sub.Value.ToQueueProperties(this, _topic);
    }

    /// <summary>
    /// Deletes the subscription, and the queue it forwards to when that is the queue KnightBus
    /// created for it. A receiver left with only the queue would wait on it and never recreate the
    /// subscription.
    /// </summary>
    public async Task Delete(string path, CancellationToken ct)
    {
        SubscriptionProperties subscription = await _adminClient
            .GetSubscriptionAsync(_topic, path, ct)
            .ConfigureAwait(false);
        await _adminClient.DeleteSubscriptionAsync(_topic, path, ct).ConfigureAwait(false);

        var forwardingQueue = SubscriptionForwarding.QueueName(_topic, path);
        if (
            !string.Equals(
                SubscriptionForwarding.EntityName(subscription.ForwardTo),
                forwardingQueue,
                StringComparison.OrdinalIgnoreCase
            )
        )
            return;

        try
        {
            await _adminClient.DeleteQueueAsync(forwardingQueue, ct).ConfigureAwait(false);
        }
        catch (ServiceBusException e)
            when (e.Reason == ServiceBusFailureReason.MessagingEntityNotFound)
        {
            //Already gone
        }
    }

    public async Task<IReadOnlyList<QueueMessage>> Peek(
        string name,
        int count,
        CancellationToken ct
    )
    {
        var receiver = _client.CreateReceiver(_topic, name);
        var messages = await receiver
            .PeekMessagesAsync(count, cancellationToken: ct)
            .ConfigureAwait(false);
        return messages
            .Select(m =>
            {
                m.ApplicationProperties.TryGetValue("Exception", out var error);
                return new QueueMessage(
                    Encoding.UTF8.GetString(m.Body),
                    error?.ToString() ?? string.Empty,
                    m.EnqueuedTime,
                    m.ScheduledEnqueueTime != default ? m.ScheduledEnqueueTime : null,
                    m.DeliveryCount,
                    m.MessageId,
                    m.ApplicationProperties
                );
            })
            .ToList();
    }

    public Task<IReadOnlyList<QueueMessage>> PeekScheduled(
        string name,
        int count,
        CancellationToken ct
    )
    {
        throw new System.NotImplementedException();
    }

    public async Task<IReadOnlyList<QueueMessage>> PeekDeadLetter(
        string path,
        int count,
        CancellationToken ct
    )
    {
        var receiver = _client.CreateReceiver(
            _topic,
            path,
            new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter }
        );
        var messages = await receiver
            .PeekMessagesAsync(count, cancellationToken: ct)
            .ConfigureAwait(false);
        return messages
            .Select(m =>
            {
                m.ApplicationProperties.TryGetValue("Exception", out var error);
                return new QueueMessage(
                    Encoding.UTF8.GetString(m.Body),
                    error?.ToString() ?? string.Empty,
                    m.EnqueuedTime,
                    m.ScheduledEnqueueTime != default ? m.ScheduledEnqueueTime : null,
                    m.DeliveryCount,
                    m.MessageId,
                    m.ApplicationProperties
                );
            })
            .ToList();
    }

    public async Task<IReadOnlyList<QueueMessage>> ReadDeadLetter(
        string path,
        int count,
        CancellationToken ct
    )
    {
        await using var receiver = _client.CreateReceiver(
            _topic,
            path,
            new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter }
        );
        return await ServiceBusQueueManager
            .ReceiveAndComplete(receiver, count, ct)
            .ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<QueueMessage>> Receive(
        string path,
        int count,
        CancellationToken ct
    )
    {
        await using var receiver = _client.CreateReceiver(_topic, path);
        return await ServiceBusQueueManager
            .ReceiveAndComplete(receiver, count, ct)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Moves the subscription's dead letters to the entity it forwards to, so that only this
    /// subscription sees them again. A subscription that does not forward gets them back through the
    /// topic, which delivers them to every subscription of the topic.
    /// </summary>
    /// <remarks>
    /// Messages dead-lettered by a receiver of a forwarding subscription are in the dead letter queue
    /// of the queue it forwards to, and are moved with <see cref="ServiceBusQueueManager"/>.
    /// </remarks>
    public async Task<int> MoveDeadLetters(string path, int count, CancellationToken ct)
    {
        SubscriptionProperties subscription = await _adminClient
            .GetSubscriptionAsync(_topic, path, ct)
            .ConfigureAwait(false);
        var destination = SubscriptionForwarding.EntityName(subscription.ForwardTo) ?? _topic;

        await using var receiver = _client.CreateReceiver(
            _topic,
            path,
            new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter }
        );
        await using var sender = _client.CreateSender(destination);

        return await ServiceBusQueueManager
            .MoveMessages(sender, receiver, count, 10)
            .ConfigureAwait(false);
    }

    public QueueType QueueType => QueueType.Subscription;
}
