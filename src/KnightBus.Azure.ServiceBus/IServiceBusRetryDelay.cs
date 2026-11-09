using System;
using KnightBus.Core;

namespace KnightBus.Azure.ServiceBus;

/// <summary>
/// Implement this on an <see cref="IProcessingSettings"/> to wait before a failed message is
/// delivered again, instead of redelivering it right away.
/// </summary>
/// <remarks>
/// Service Bus has no way to abandon a message with a delay, so a failed message is scheduled again
/// as a copy, <see cref="GetRetryDelay"/> into the future, and the original is completed. The copy
/// carries the number of earlier deliveries in <see cref="ServiceBusMessageProperties.PreviousDeliveryCount"/>,
/// which keeps <see cref="IProcessingSettings.DeadLetterDeliveryLimit"/> counting across the copies.
/// The copy gets a new message id, with the first one in <see cref="ServiceBusMessageProperties.OriginalMessageId"/>,
/// so duplicate detection on the queue cannot drop it.
/// Only queues (commands) are retried with a delay. A subscription cannot be sent to on its own, so
/// a failed event is abandoned and redelivered right away as before.
/// </remarks>
public interface IServiceBusRetryDelay
{
    /// <summary>
    /// How long to wait before the message is delivered again.
    /// Return <see cref="TimeSpan.Zero"/> to redeliver it right away.
    /// </summary>
    /// <param name="deliveryCount">
    /// The number of times the message has been delivered, the one that just failed included:
    /// 1 after the first failure.
    /// </param>
    TimeSpan GetRetryDelay(int deliveryCount);
}

/// <summary>
/// Application properties KnightBus sets on Service Bus messages
/// </summary>
public static class ServiceBusMessageProperties
{
    /// <summary>
    /// The number of times earlier copies of a message rescheduled by <see cref="IServiceBusRetryDelay"/>
    /// were delivered. Added to the broker's own delivery count.
    /// </summary>
    public const string PreviousDeliveryCount = "KnightBus.PreviousDeliveryCount";

    /// <summary>
    /// The message id of the message a copy rescheduled by <see cref="IServiceBusRetryDelay"/> was made from
    /// </summary>
    public const string OriginalMessageId = "KnightBus.OriginalMessageId";
}
