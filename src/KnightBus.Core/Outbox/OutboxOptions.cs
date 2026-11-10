using System;

namespace KnightBus.Core.Outbox;

/// <summary>
/// Settings of the <see cref="OutboxDispatcher"/>.
/// </summary>
public class OutboxOptions
{
    /// <summary>
    /// Whether this process runs a dispatcher. Turn it off in processes that only write to the
    /// outbox, as long as some other process dispatches from the same store.
    /// </summary>
    /// <remarks>Defaults to true.</remarks>
    public bool EnableDispatcher { get; set; } = true;

    /// <summary>
    /// The most messages the dispatcher locks and sends at a time.
    /// </summary>
    /// <remarks>Defaults to 100.</remarks>
    public int BatchSize { get; set; } = 100;

    /// <summary>
    /// How long the dispatcher waits before looking again once the outbox has been emptied. It is
    /// also the longest a committed message waits before it is dispatched.
    /// </summary>
    /// <remarks>Defaults to 1 second.</remarks>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// How long a batch stays locked to the dispatcher that took it. It must cover sending the
    /// batch; if it runs out first, another dispatcher may send the same messages again.
    /// </summary>
    /// <remarks>Defaults to 1 minute.</remarks>
    public TimeSpan LockDuration { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// How many times a message is tried before it is marked as failed and left in the store.
    /// </summary>
    /// <remarks>Defaults to 10.</remarks>
    public int MaxAttempts { get; set; } = 10;

    /// <summary>
    /// Wait before the first retry of a message that could not be sent. It doubles with every
    /// attempt, up to <see cref="MaxRetryDelay"/>.
    /// </summary>
    /// <remarks>Defaults to 1 second.</remarks>
    public TimeSpan MinRetryDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// The longest wait between two attempts to send a message.
    /// </summary>
    /// <remarks>Defaults to 5 minutes.</remarks>
    public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromMinutes(5);

    internal TimeSpan GetRetryDelay(int attempts)
    {
        var exponent = Math.Clamp(attempts - 1, 0, 30);
        var delay = MinRetryDelay.TotalMilliseconds * Math.Pow(2, exponent);
        return delay >= MaxRetryDelay.TotalMilliseconds
            ? MaxRetryDelay
            : TimeSpan.FromMilliseconds(delay);
    }

    public virtual void Validate()
    {
        if (BatchSize < 1)
            throw new ArgumentOutOfRangeException(
                nameof(BatchSize),
                BatchSize,
                $"{nameof(BatchSize)} must be at least 1"
            );
        if (PollInterval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(
                nameof(PollInterval),
                PollInterval,
                $"{nameof(PollInterval)} must be positive"
            );
        if (LockDuration <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(
                nameof(LockDuration),
                LockDuration,
                $"{nameof(LockDuration)} must be positive"
            );
        if (MaxAttempts < 1)
            throw new ArgumentOutOfRangeException(
                nameof(MaxAttempts),
                MaxAttempts,
                $"{nameof(MaxAttempts)} must be at least 1"
            );
        if (MinRetryDelay <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(
                nameof(MinRetryDelay),
                MinRetryDelay,
                $"{nameof(MinRetryDelay)} must be positive"
            );
        if (MaxRetryDelay < MinRetryDelay)
            throw new ArgumentException(
                $"{nameof(MaxRetryDelay)} ({MaxRetryDelay}) must not be shorter than {nameof(MinRetryDelay)} ({MinRetryDelay})"
            );
    }
}
