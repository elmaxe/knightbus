using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;

namespace KnightBus.Azure.ServiceBus;

/// <summary>
/// What a queue receiver needs to reschedule a failed message, see <see cref="IServiceBusRetryDelay"/>
/// </summary>
internal sealed class ServiceBusDelayedRetry
{
    public ServiceBusDelayedRetry(
        ServiceBusSender sender,
        IServiceBusRetryDelay policy,
        ILogger log
    )
    {
        Sender = sender;
        Policy = policy;
        Log = log;
    }

    /// <summary>
    /// Sends to the queue the message was received from, on the same connection as the receiver
    /// </summary>
    public ServiceBusSender Sender { get; }
    public IServiceBusRetryDelay Policy { get; }
    public ILogger Log { get; }
}
