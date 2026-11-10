using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AwesomeAssertions;
using KnightBus.Core.Outbox;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using NUnit.Framework;

namespace KnightBus.Core.Tests.Unit;

[TestFixture]
public class OutboxDispatcherTests
{
    private Mock<IOutboxStore> _store = null!;
    private Mock<IOutboxTransport> _transport = null!;
    private OutboxOptions _options = null!;
    private List<IReadOnlyList<OutboxMessage>> _sent = null!;

    [SetUp]
    public void Setup()
    {
        _store = new Mock<IOutboxStore>();
        _transport = new Mock<IOutboxTransport>();
        _options = new OutboxOptions { BatchSize = 10, MaxAttempts = 3 };
        _sent = new List<IReadOnlyList<OutboxMessage>>();
        _transport
            .Setup(t =>
                t.SendAsync(It.IsAny<IReadOnlyList<OutboxMessage>>(), It.IsAny<CancellationToken>())
            )
            .Callback<IReadOnlyList<OutboxMessage>, CancellationToken>((m, _) => _sent.Add(m))
            .Returns(Task.CompletedTask);
    }

    private OutboxDispatcher CreateDispatcher() =>
        new(_store.Object, _transport.Object, _options, NullLogger<OutboxDispatcher>.Instance);

    private void GivenLocked(params OutboxRecord[] records) =>
        _store
            .Setup(s =>
                s.LockAsync(
                    _options.BatchSize,
                    _options.LockDuration,
                    It.IsAny<CancellationToken>()
                )
            )
            .ReturnsAsync(records);

    private static OutboxRecord Record(string destination, int attempts = 1) =>
        new(
            new OutboxMessage
            {
                Id = Guid.NewGuid(),
                Destination = destination,
                Body = [1, 2, 3],
            },
            attempts,
            Guid.NewGuid()
        );

    [Test]
    public async Task Does_nothing_when_no_message_is_due()
    {
        GivenLocked();

        var dispatched = await CreateDispatcher().DispatchOnceAsync(CancellationToken.None);

        dispatched.Should().Be(0);
        _sent.Should().BeEmpty();
        _store.Verify(
            s =>
                s.CompleteAsync(
                    It.IsAny<IReadOnlyList<OutboxRecord>>(),
                    It.IsAny<CancellationToken>()
                ),
            Times.Never
        );
    }

    [Test]
    public async Task Sends_each_destination_together_and_completes_the_messages()
    {
        var a1 = Record("a");
        var b1 = Record("b");
        var a2 = Record("a");
        GivenLocked(a1, b1, a2);

        var dispatched = await CreateDispatcher().DispatchOnceAsync(CancellationToken.None);

        dispatched.Should().Be(3);
        _sent.Should().HaveCount(2);
        _sent[0].Should().Equal(a1.Message, a2.Message);
        _sent[1].Should().Equal(b1.Message);
        _store.Verify(
            s =>
                s.CompleteAsync(
                    It.Is<IReadOnlyList<OutboxRecord>>(r => r.SequenceEqual(new[] { a1, a2 })),
                    It.IsAny<CancellationToken>()
                ),
            Times.Once
        );
        _store.Verify(
            s =>
                s.CompleteAsync(
                    It.Is<IReadOnlyList<OutboxRecord>>(r => r.SequenceEqual(new[] { b1 })),
                    It.IsAny<CancellationToken>()
                ),
            Times.Once
        );
    }

    [Test]
    public async Task A_message_that_cannot_be_sent_does_not_hold_back_the_others()
    {
        var good = Record("a");
        var bad = Record("a");
        GivenLocked(good, bad);
        var error = new InvalidOperationException("too large");
        _transport
            .Setup(t =>
                t.SendAsync(
                    It.Is<IReadOnlyList<OutboxMessage>>(m => m.Contains(bad.Message)),
                    It.IsAny<CancellationToken>()
                )
            )
            .ThrowsAsync(error);

        await CreateDispatcher().DispatchOnceAsync(CancellationToken.None);

        _store.Verify(
            s =>
                s.CompleteAsync(
                    It.Is<IReadOnlyList<OutboxRecord>>(r => r.SequenceEqual(new[] { good })),
                    It.IsAny<CancellationToken>()
                ),
            Times.Once
        );
        _store.Verify(
            s =>
                s.RetryLaterAsync(
                    bad,
                    error,
                    _options.MinRetryDelay,
                    It.IsAny<CancellationToken>()
                ),
            Times.Once
        );
        _store.Verify(
            s =>
                s.FailAsync(
                    It.IsAny<OutboxRecord>(),
                    It.IsAny<Exception>(),
                    It.IsAny<CancellationToken>()
                ),
            Times.Never
        );
    }

    [Test]
    public async Task Waits_longer_after_each_failed_attempt()
    {
        var record = Record("a", attempts: 2);
        GivenLocked(record);
        var error = new Exception("unavailable");
        _transport
            .Setup(t =>
                t.SendAsync(It.IsAny<IReadOnlyList<OutboxMessage>>(), It.IsAny<CancellationToken>())
            )
            .ThrowsAsync(error);

        await CreateDispatcher().DispatchOnceAsync(CancellationToken.None);

        _store.Verify(
            s =>
                s.RetryLaterAsync(
                    record,
                    error,
                    _options.MinRetryDelay * 2,
                    It.IsAny<CancellationToken>()
                ),
            Times.Once
        );
    }

    [Test]
    public async Task Fails_a_message_on_its_last_attempt()
    {
        var record = Record("a", attempts: _options.MaxAttempts);
        GivenLocked(record);
        var error = new Exception("unavailable");
        _transport
            .Setup(t =>
                t.SendAsync(It.IsAny<IReadOnlyList<OutboxMessage>>(), It.IsAny<CancellationToken>())
            )
            .ThrowsAsync(error);

        await CreateDispatcher().DispatchOnceAsync(CancellationToken.None);

        _store.Verify(s => s.FailAsync(record, error, It.IsAny<CancellationToken>()), Times.Once);
        _store.Verify(
            s =>
                s.RetryLaterAsync(
                    It.IsAny<OutboxRecord>(),
                    It.IsAny<Exception>(),
                    It.IsAny<TimeSpan>(),
                    It.IsAny<CancellationToken>()
                ),
            Times.Never
        );
    }

    [Test]
    public async Task Leaves_the_messages_locked_when_stopped_while_sending()
    {
        var record = Record("a");
        GivenLocked(record);
        using var cts = new CancellationTokenSource();
        _transport
            .Setup(t =>
                t.SendAsync(It.IsAny<IReadOnlyList<OutboxMessage>>(), It.IsAny<CancellationToken>())
            )
            .Returns<IReadOnlyList<OutboxMessage>, CancellationToken>(
                (_, ct) =>
                {
                    cts.Cancel();
                    return Task.FromCanceled(ct);
                }
            );

        var act = () => CreateDispatcher().DispatchOnceAsync(cts.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
        //Nothing completed or released: the lock runs out and the message is sent later
        _store.Verify(
            s => s.LockAsync(It.IsAny<int>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()),
            Times.Once
        );
        _store.VerifyNoOtherCalls();
    }

    [Test]
    public async Task Keeps_dispatching_until_stopped()
    {
        _options.PollInterval = TimeSpan.FromMilliseconds(10);
        var record = Record("a");
        var locks = 0;
        _store
            .Setup(s =>
                s.LockAsync(It.IsAny<int>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>())
            )
            .ReturnsAsync(() =>
                Interlocked.Increment(ref locks) == 2 ? [record] : Array.Empty<OutboxRecord>()
            );
        var dispatcher = CreateDispatcher();

        await dispatcher.StartAsync(CancellationToken.None);
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (_sent.Count == 0 && DateTime.UtcNow < deadline)
            await Task.Delay(10);
        await dispatcher.StopAsync(CancellationToken.None);

        _sent.Should().ContainSingle().Which.Should().Equal(record.Message);
    }

    [Test]
    public void Retry_delay_doubles_up_to_the_maximum()
    {
        var options = new OutboxOptions
        {
            MinRetryDelay = TimeSpan.FromSeconds(1),
            MaxRetryDelay = TimeSpan.FromSeconds(5),
        };

        Enumerable
            .Range(1, 5)
            .Select(options.GetRetryDelay)
            .Should()
            .Equal(
                TimeSpan.FromSeconds(1),
                TimeSpan.FromSeconds(2),
                TimeSpan.FromSeconds(4),
                TimeSpan.FromSeconds(5),
                TimeSpan.FromSeconds(5)
            );
    }

    [Test]
    public void Rejects_invalid_options()
    {
        var act = () => new OutboxOptions { BatchSize = 0 }.Validate();
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("BatchSize");

        act = () =>
            new OutboxOptions
            {
                MinRetryDelay = TimeSpan.FromSeconds(2),
                MaxRetryDelay = TimeSpan.FromSeconds(1),
            }.Validate();
        act.Should().Throw<ArgumentException>();
    }
}
