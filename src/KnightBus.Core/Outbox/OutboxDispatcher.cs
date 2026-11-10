using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace KnightBus.Core.Outbox;

/// <summary>
/// Sends the messages written to an <see cref="IOutboxStore"/> through an
/// <see cref="IOutboxTransport"/>. Delivery is at least once: a message can be sent again if the
/// dispatcher stops between sending it and removing it from the store, so receivers should be
/// idempotent or the broker should drop duplicates by message id.
/// </summary>
/// <remarks>
/// Any number of dispatchers can share a store, the store's locks keep them from sending the same
/// message at the same time. Messages are sent oldest first, but with more than one dispatcher, or
/// after a retry, they can arrive out of order.
/// </remarks>
public sealed class OutboxDispatcher : BackgroundService
{
    private readonly IOutboxStore _store;
    private readonly IOutboxTransport _transport;
    private readonly OutboxOptions _options;
    private readonly ILogger<OutboxDispatcher> _logger;

    public OutboxDispatcher(
        IOutboxStore store,
        IOutboxTransport transport,
        OutboxOptions options,
        ILogger<OutboxDispatcher> logger
    )
    {
        options.Validate();
        _store = store;
        _transport = transport;
        _options = options;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Outbox dispatcher started");
        while (!stoppingToken.IsCancellationRequested)
        {
            int dispatched;
            try
            {
                dispatched = await DispatchOnceAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception e)
            {
                //Locked messages are retried once their lock runs out
                _logger.LogError(e, "Outbox dispatch failed");
                dispatched = 0;
            }

            //A full batch means there may be more waiting
            if (dispatched >= _options.BatchSize)
                continue;

            try
            {
                await Task.Delay(_options.PollInterval, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
        _logger.LogInformation("Outbox dispatcher stopped");
    }

    /// <summary>
    /// Locks one batch of due messages and sends it.
    /// </summary>
    /// <returns>The number of messages locked, sent or not.</returns>
    public async Task<int> DispatchOnceAsync(CancellationToken cancellationToken)
    {
        var records = await _store
            .LockAsync(_options.BatchSize, _options.LockDuration, cancellationToken)
            .ConfigureAwait(false);
        if (records.Count == 0)
            return 0;

        foreach (
            var destination in records.GroupBy(r => r.Message.Destination, StringComparer.Ordinal)
        )
        {
            await DispatchAsync(destination.ToList(), cancellationToken).ConfigureAwait(false);
        }

        return records.Count;
    }

    private async Task DispatchAsync(
        IReadOnlyList<OutboxRecord> records,
        CancellationToken cancellationToken
    )
    {
        try
        {
            await _transport
                .SendAsync(records.Select(r => r.Message).ToList(), cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception e) when (!cancellationToken.IsCancellationRequested)
        {
            if (records.Count == 1)
            {
                await HandleFailureAsync(records[0], e).ConfigureAwait(false);
                return;
            }

            //One bad message must not hold back the rest, so try them one by one
            _logger.LogWarning(
                e,
                "Could not send {MessageCount} outbox messages to {Destination}, sending them one by one",
                records.Count,
                records[0].Message.Destination
            );
            foreach (var record in records)
            {
                await DispatchAsync([record], cancellationToken).ConfigureAwait(false);
            }
            return;
        }

        //The messages are sent, so stopping now would only send them again
        await _store.CompleteAsync(records, CancellationToken.None).ConfigureAwait(false);
    }

    private async Task HandleFailureAsync(OutboxRecord record, Exception error)
    {
        if (record.Attempts >= _options.MaxAttempts)
        {
            _logger.LogError(
                error,
                "Outbox message {MessageId} to {Destination} failed after {Attempts} attempts and will not be sent",
                record.Message.Id,
                record.Message.Destination,
                record.Attempts
            );
            await _store.FailAsync(record, error, CancellationToken.None).ConfigureAwait(false);
            return;
        }

        var delay = _options.GetRetryDelay(record.Attempts);
        _logger.LogWarning(
            error,
            "Outbox message {MessageId} to {Destination} failed on attempt {Attempts}, retrying in {RetryDelay}",
            record.Message.Id,
            record.Message.Destination,
            record.Attempts,
            delay
        );
        await _store
            .RetryLaterAsync(record, error, delay, CancellationToken.None)
            .ConfigureAwait(false);
    }
}
