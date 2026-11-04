using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using KnightBus.Core;
using KnightBus.Messages;
using Microsoft.Extensions.Logging;

namespace KnightBus.Azure.ServiceBus;

internal static class ServiceBusEntities
{
    public static IServiceBusCreationOptions GetCreationOptions<T>(
        IServiceBusConfiguration configuration
    )
        where T : IMessage
    {
        var queueMapping = AutoMessageMapper.GetMapping<T>();
        // ReSharper disable once SuspiciousTypeConversion.Global
        return queueMapping as IServiceBusCreationOptions ?? configuration.DefaultCreationOptions;
    }

    public static async Task CreateQueue(
        ServiceBusAdministrationClient client,
        string queueName,
        IServiceBusCreationOptions options,
        ILogger log,
        CancellationToken cancellationToken
    )
    {
        try
        {
            await client
                .CreateQueueAsync(
                    new CreateQueueOptions(queueName)
                    {
                        EnablePartitioning = options.EnablePartitioning,
                        EnableBatchedOperations = options.EnableBatchedOperations,
                    },
                    cancellationToken
                )
                .ConfigureAwait(false);
        }
        catch (ServiceBusException e)
            when (e.Reason == ServiceBusFailureReason.MessagingEntityAlreadyExists)
        {
            //Swallow
        }
        catch (ServiceBusException e)
        {
            log.LogError(e, "Failed to create queue {QueueName}", queueName);
            throw;
        }
    }

    public static async Task CreateTopic(
        ServiceBusAdministrationClient client,
        string topicName,
        IServiceBusCreationOptions options,
        ILogger log,
        CancellationToken cancellationToken
    )
    {
        try
        {
            await client
                .CreateTopicAsync(
                    new CreateTopicOptions(topicName)
                    {
                        EnablePartitioning = options.EnablePartitioning,
                        EnableBatchedOperations = options.EnableBatchedOperations,
                        SupportOrdering = options.SupportOrdering,
                    },
                    cancellationToken
                )
                .ConfigureAwait(false);
        }
        catch (ServiceBusException e)
            when (e.Reason == ServiceBusFailureReason.MessagingEntityAlreadyExists)
        {
            //Swallow
        }
        catch (ServiceBusException e)
        {
            log.LogError(e, "Failed to create topic {TopicName}", topicName);
            throw;
        }
    }

    /// <summary>
    /// Creates the topic and the subscription, and when the options ask for it the queue the
    /// subscription forwards to. A subscription that already exists is pointed at that queue, which
    /// is how subscriptions created before forwarding was enabled are moved over.
    /// </summary>
    public static async Task CreateSubscription(
        ServiceBusAdministrationClient client,
        string topicName,
        string subscriptionName,
        IServiceBusCreationOptions options,
        ILogger log,
        CancellationToken cancellationToken
    )
    {
        await CreateTopic(client, topicName, options, log, cancellationToken).ConfigureAwait(false);

        string? forwardTo = null;
        if (options.ForwardSubscriptionsToQueue)
        {
            forwardTo = SubscriptionForwarding.QueueName(topicName, subscriptionName);
            //Service Bus refuses to forward to an entity that does not exist yet
            await CreateQueue(client, forwardTo, options, log, cancellationToken)
                .ConfigureAwait(false);
        }

        try
        {
            await client
                .CreateSubscriptionAsync(
                    new CreateSubscriptionOptions(topicName, subscriptionName)
                    {
                        EnableBatchedOperations = options.EnableBatchedOperations,
                        ForwardTo = forwardTo,
                    },
                    cancellationToken
                )
                .ConfigureAwait(false);
        }
        catch (ServiceBusException e)
            when (e.Reason == ServiceBusFailureReason.MessagingEntityAlreadyExists)
        {
            if (forwardTo is not null)
                await EnsureForwarding(
                        client,
                        topicName,
                        subscriptionName,
                        forwardTo,
                        log,
                        cancellationToken
                    )
                    .ConfigureAwait(false);
        }
        catch (ServiceBusException e)
        {
            log.LogError(
                e,
                "Failed to create subscription {TopicName} {SubscriptionName}",
                topicName,
                subscriptionName
            );
            throw;
        }
    }

    private static async Task EnsureForwarding(
        ServiceBusAdministrationClient client,
        string topicName,
        string subscriptionName,
        string forwardTo,
        ILogger log,
        CancellationToken cancellationToken
    )
    {
        try
        {
            SubscriptionProperties subscription = await client
                .GetSubscriptionAsync(topicName, subscriptionName, cancellationToken)
                .ConfigureAwait(false);
            if (SubscriptionForwarding.IsForwardingTo(subscription.ForwardTo, forwardTo))
                return;

            log.LogInformation(
                "Forwarding subscription {TopicName} {SubscriptionName} to queue {QueueName}, it forwarded to {PreviousForwardTo}",
                topicName,
                subscriptionName,
                forwardTo,
                SubscriptionForwarding.EntityName(subscription.ForwardTo) ?? "nothing"
            );
            subscription.ForwardTo = forwardTo;
            await client
                .UpdateSubscriptionAsync(subscription, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (ServiceBusException e)
        {
            log.LogError(
                e,
                "Failed to forward subscription {TopicName} {SubscriptionName} to queue {QueueName}",
                topicName,
                subscriptionName,
                forwardTo
            );
            throw;
        }
    }
}
