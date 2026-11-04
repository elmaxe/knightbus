using System;

namespace KnightBus.Azure.ServiceBus;

/// <summary>
/// Naming of the queues that subscriptions forward to when
/// <see cref="IServiceBusCreationOptions.ForwardSubscriptionsToQueue"/> is enabled.
/// </summary>
public static class SubscriptionForwarding
{
    /// <summary>
    /// The queue a subscription forwards to: <c>{topic}.{subscription}</c>.
    /// </summary>
    public static string QueueName(string topic, string subscription) => $"{topic}.{subscription}";

    /// <summary>
    /// The name of the entity a <c>ForwardTo</c> value points to, or null when it points nowhere.
    /// Service Bus reports the value back as an address such as
    /// <c>sb://yournamespace.servicebus.windows.net/queue</c>, which is reduced to <c>queue</c>.
    /// </summary>
    public static string? EntityName(string? forwardTo)
    {
        if (string.IsNullOrWhiteSpace(forwardTo))
            return null;

        var name = Uri.TryCreate(forwardTo, UriKind.Absolute, out var uri)
            ? uri.AbsolutePath
            : forwardTo;
        name = name.Trim('/');
        return name.Length == 0 ? null : name;
    }

    internal static bool IsForwardingTo(string? forwardTo, string queueName) =>
        string.Equals(EntityName(forwardTo), queueName, StringComparison.OrdinalIgnoreCase);
}
