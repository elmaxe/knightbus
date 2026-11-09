using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using KnightBus.Core;
using KnightBus.Messages;
using Microsoft.Extensions.Logging;

namespace KnightBus.Azure.ServiceBus;

internal class ServiceBusMessageStateHandler<T> : IMessageStateHandler<T>
    where T : class, IMessage
{
    private readonly ProcessMessageEventArgs _processMessage;
    private readonly ServiceBusReceivedMessage _sbMessage;
    private readonly T _message;
    private readonly ServiceBusDelayedRetry? _delayedRetry;
    private readonly int _previousDeliveryCount;

    public ServiceBusMessageStateHandler(
        ProcessMessageEventArgs processMessage,
        IMessageSerializer serializer,
        int deadLetterDeliveryLimit,
        IDependencyInjection messageScope,
        ServiceBusDelayedRetry? delayedRetry = null
    )
    {
        DeadLetterDeliveryLimit = deadLetterDeliveryLimit;
        MessageScope = messageScope;
        _processMessage = processMessage;
        _sbMessage = processMessage.Message;
        _delayedRetry = delayedRetry;
        _previousDeliveryCount = ReadPreviousDeliveryCount(_sbMessage);
        _message = serializer.Deserialize<T>(_sbMessage.Body.ToMemory());
    }

    //A message rescheduled by a delayed retry is a new message to the broker, which counts its
    //deliveries from zero, so the deliveries of the copies before it are added on top
    public int DeliveryCount => _sbMessage.DeliveryCount + _previousDeliveryCount;
    public int DeadLetterDeliveryLimit { get; }
    public IDictionary<string, string> MessageProperties =>
        _sbMessage
            .ApplicationProperties?.Where(kvp => kvp.Value is string)
            .ToDictionary(k => k.Key, k => k.Value.ToString()!) ?? new Dictionary<string, string>();

    public async Task CompleteAsync()
    {
        await _processMessage.CompleteMessageAsync(_sbMessage).ConfigureAwait(false);
    }

    public Task ReplyAsync<TReply>(TReply reply)
    {
        throw new NotImplementedException();
    }

    public async Task AbandonByErrorAsync(Exception e)
    {
        var delay = GetRetryDelay(e);
        if (delay > TimeSpan.Zero && await TryRescheduleAsync(e, delay).ConfigureAwait(false))
            return;

        await _sbMessage.AbandonByErrorAsync(_processMessage, e).ConfigureAwait(false);
    }

    private TimeSpan GetRetryDelay(Exception e)
    {
        if (_delayedRetry is null)
            return TimeSpan.Zero;
        //Processing was interrupted, by a shutdown or a lost lock, rather than failed, so the
        //message is handed back right away
        if (e is OperationCanceledException)
            return TimeSpan.Zero;
        //The next delivery dead-letters the message without processing it, so waiting for it
        //only delays the dead-lettering
        if (DeliveryCount >= DeadLetterDeliveryLimit)
            return TimeSpan.Zero;
        return _delayedRetry.Policy.GetRetryDelay(DeliveryCount);
    }

    /// <summary>
    /// Schedules a copy of the message <paramref name="delay"/> into the future and completes the
    /// original. Returns false, with the original untouched, when the copy cannot be scheduled.
    /// </summary>
    private async Task<bool> TryRescheduleAsync(Exception e, TimeSpan delay)
    {
        var retry = _delayedRetry!;
        long sequenceNumber;
        try
        {
            sequenceNumber = await retry
                .Sender.ScheduleMessageAsync(CreateRetryMessage(e), DateTimeOffset.UtcNow + delay)
                .ConfigureAwait(false);
        }
        catch (Exception scheduleError)
        {
            retry.Log.LogWarning(
                scheduleError,
                "Failed to schedule a delayed retry of {MessageType}, abandoning it instead",
                typeof(T).Name
            );
            return false;
        }

        try
        {
            await _processMessage.CompleteMessageAsync(_sbMessage).ConfigureAwait(false);
        }
        catch
        {
            //The original stays on the queue and is delivered again, so the copy must not be
            try
            {
                await retry
                    .Sender.CancelScheduledMessageAsync(sequenceNumber)
                    .ConfigureAwait(false);
            }
            catch (Exception cancelError)
            {
                retry.Log.LogWarning(
                    cancelError,
                    "Failed to cancel the delayed retry {SequenceNumber} of {MessageType}, which may be processed twice",
                    sequenceNumber,
                    typeof(T).Name
                );
            }
            throw;
        }
        return true;
    }

    private ServiceBusMessage CreateRetryMessage(Exception e)
    {
        //A new id, or duplicate detection on the queue would drop the copy of a completed message
        var retry = new ServiceBusMessage(_sbMessage) { MessageId = Guid.NewGuid().ToString() };
        foreach (var property in ServiceBusMessageExtensions.ErrorProperties(e))
            retry.ApplicationProperties[property.Key] = property.Value;
        retry.ApplicationProperties[ServiceBusMessageProperties.PreviousDeliveryCount] =
            DeliveryCount;
        if (!retry.ApplicationProperties.ContainsKey(ServiceBusMessageProperties.OriginalMessageId))
            retry.ApplicationProperties[ServiceBusMessageProperties.OriginalMessageId] =
                _sbMessage.MessageId;
        return retry;
    }

    private static int ReadPreviousDeliveryCount(ServiceBusReceivedMessage message)
    {
        if (
            message.ApplicationProperties is null
            || !message.ApplicationProperties.TryGetValue(
                ServiceBusMessageProperties.PreviousDeliveryCount,
                out var value
            )
        )
            return 0;
        var count = value switch
        {
            int i => i,
            long l => (int)Math.Min(l, int.MaxValue),
            string s when int.TryParse(s, out var parsed) => parsed,
            _ => 0,
        };
        return Math.Max(count, 0);
    }

    public async Task DeadLetterAsync(int deadLetterLimit)
    {
        await _sbMessage
            .DeadLetterByDeliveryLimitAsync(_processMessage, deadLetterLimit)
            .ConfigureAwait(false);
    }

    public T GetMessage()
    {
        return _message;
    }

    public IDependencyInjection MessageScope { get; set; }
}
