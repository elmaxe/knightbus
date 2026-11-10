using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AwesomeAssertions;
using KnightBus.Core.Outbox;
using KnightBus.SqlServer.Outbox;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

namespace KnightBus.SqlServer.Tests.Integration;

[TestFixture]
public class SqlServerOutboxTests
{
    private SqlServerOutboxOptions _options = null!;
    private SqlServerOutboxStore _store = null!;

    [SetUp]
    public async Task Setup()
    {
        //Every test gets its own table
        _options = new SqlServerOutboxOptions { TableName = $"Outbox_{Guid.NewGuid():N}" };
        await SqlServerOutboxSchema.EnsureCreatedAsync(SqlServerSetup.ConnectionString, _options);
        _store = new SqlServerOutboxStore(SqlServerSetup.ConnectionString, _options);
    }

    private static OutboxMessage Message(string destination = "queue") =>
        new()
        {
            Id = Guid.CreateVersion7(),
            Destination = destination,
            Body = [1, 2, 3],
            ContentType = "application/json",
            Properties = new Dictionary<string, string> { ["traceparent"] = "00-abc-01" },
            ScheduledEnqueueTimeUtc = new DateTimeOffset(2030, 1, 2, 3, 4, 5, TimeSpan.Zero),
        };

    private async Task WriteAsync(bool commit, params OutboxMessage[] messages)
    {
        await using var connection = new SqlConnection(SqlServerSetup.ConnectionString);
        await connection.OpenAsync();
        await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync();
        var session = new SqlServerOutboxSession(_options);
        session.UseSqlTransaction(transaction);
        await session.AddAsync(messages, CancellationToken.None);
        if (commit)
            await transaction.CommitAsync();
        else
            await transaction.RollbackAsync();
    }

    private Task<IReadOnlyList<OutboxRecord>> LockAsync(int max = 100, TimeSpan? duration = null) =>
        _store.LockAsync(max, duration ?? TimeSpan.FromMinutes(1), CancellationToken.None);

    [Test]
    public async Task Committed_messages_are_handed_out_as_written()
    {
        var message = Message();
        await WriteAsync(commit: true, message);

        var locked = await LockAsync();

        var record = locked.Should().ContainSingle().Subject;
        record.Attempts.Should().Be(1);
        record.Message.Should().BeEquivalentTo(message);
    }

    [Test]
    public async Task Rolled_back_messages_are_never_handed_out()
    {
        await WriteAsync(commit: false, Message());

        (await LockAsync()).Should().BeEmpty();
    }

    [Test]
    public async Task Messages_are_handed_out_oldest_first_and_at_most_a_batch_at_a_time()
    {
        var messages = Enumerable.Range(0, 5).Select(_ => Message()).ToArray();
        await WriteAsync(commit: true, messages);

        var first = await LockAsync(max: 3);
        var second = await LockAsync(max: 3);

        first.Select(r => r.Message.Id).Should().Equal(messages.Take(3).Select(m => m.Id));
        second.Select(r => r.Message.Id).Should().Equal(messages.Skip(3).Select(m => m.Id));
    }

    [Test]
    public async Task Writes_more_messages_than_fit_in_one_command()
    {
        var messages = Enumerable.Range(0, 1000).Select(_ => Message()).ToArray();
        await WriteAsync(commit: true, messages);

        var locked = await LockAsync(max: 2000);

        locked.Select(r => r.Message.Id).Should().Equal(messages.Select(m => m.Id));
    }

    [Test]
    public async Task Messages_being_written_are_skipped_without_waiting_for_the_transaction()
    {
        await using var connection = new SqlConnection(SqlServerSetup.ConnectionString);
        await connection.OpenAsync();
        await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync();
        var session = new SqlServerOutboxSession(_options);
        session.UseSqlTransaction(transaction);
        await session.AddAsync([Message()], CancellationToken.None);

        var stopwatch = Stopwatch.StartNew();
        var whileOpen = await LockAsync();
        stopwatch.Stop();
        await transaction.CommitAsync();

        whileOpen.Should().BeEmpty();
        stopwatch.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(5));
        (await LockAsync()).Should().ContainSingle();
    }

    [Test]
    public async Task A_locked_message_is_handed_out_again_once_its_lock_runs_out()
    {
        await WriteAsync(commit: true, Message());

        var first = await LockAsync(duration: TimeSpan.FromMilliseconds(500));
        var whileLocked = await LockAsync();
        await Task.Delay(TimeSpan.FromSeconds(1));
        var afterLock = await LockAsync();

        first.Should().ContainSingle();
        whileLocked.Should().BeEmpty();
        afterLock.Should().ContainSingle().Which.Attempts.Should().Be(2);
        afterLock[0].LockToken.Should().NotBe(first[0].LockToken);
    }

    [Test]
    public async Task Completing_removes_the_messages()
    {
        await WriteAsync(commit: true, Message(), Message());
        var locked = await LockAsync(duration: TimeSpan.FromMilliseconds(200));

        await _store.CompleteAsync(locked, CancellationToken.None);
        await Task.Delay(TimeSpan.FromMilliseconds(400));

        (await LockAsync()).Should().BeEmpty();
    }

    [Test]
    public async Task A_dispatcher_that_lost_its_lock_cannot_complete_the_message()
    {
        await WriteAsync(commit: true, Message());
        var stale = await LockAsync(duration: TimeSpan.FromMilliseconds(200));
        await Task.Delay(TimeSpan.FromMilliseconds(400));
        var current = await LockAsync(duration: TimeSpan.FromMilliseconds(200));

        await _store.CompleteAsync(stale, CancellationToken.None);
        await Task.Delay(TimeSpan.FromMilliseconds(400));

        (await LockAsync())
            .Should()
            .ContainSingle()
            .Which.Message.Id.Should()
            .Be(current[0].Message.Id);
    }

    [Test]
    public async Task A_message_to_retry_waits_for_the_delay()
    {
        await WriteAsync(commit: true, Message());
        var locked = await LockAsync();

        await _store.RetryLaterAsync(
            locked[0],
            new Exception("unavailable"),
            TimeSpan.FromMilliseconds(500),
            CancellationToken.None
        );
        var whileWaiting = await LockAsync();
        await Task.Delay(TimeSpan.FromSeconds(1));
        var afterWaiting = await LockAsync();

        whileWaiting.Should().BeEmpty();
        afterWaiting.Should().ContainSingle().Which.Attempts.Should().Be(2);
        (await ReadAsync<string>("LastError", locked[0].Message.Id))
            .Should()
            .Contain("unavailable");
    }

    [Test]
    public async Task A_failed_message_is_kept_but_never_handed_out_again()
    {
        await WriteAsync(commit: true, Message());
        var locked = await LockAsync(duration: TimeSpan.FromMilliseconds(200));

        await _store.FailAsync(locked[0], new Exception("poison"), CancellationToken.None);
        await Task.Delay(TimeSpan.FromMilliseconds(400));

        (await LockAsync()).Should().BeEmpty();
        (await ReadAsync<DateTime?>("FailedUtc", locked[0].Message.Id)).Should().NotBeNull();
    }

    [Test]
    public async Task The_session_refuses_messages_without_an_open_transaction()
    {
        var session = new SqlServerOutboxSession(_options);
        session.IsEnlisted.Should().BeFalse();
        var withoutTransaction = () => session.AddAsync([Message()], CancellationToken.None);
        await withoutTransaction.Should().ThrowAsync<OutboxNotEnlistedException>();

        await using var connection = new SqlConnection(SqlServerSetup.ConnectionString);
        await connection.OpenAsync();
        await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync();
        session.UseSqlTransaction(transaction);
        session.IsEnlisted.Should().BeTrue();
        await transaction.CommitAsync();

        session.IsEnlisted.Should().BeFalse();
        var afterCommit = () => session.AddAsync([Message()], CancellationToken.None);
        await afterCommit.Should().ThrowAsync<OutboxNotEnlistedException>();
    }

    [Test]
    public async Task The_dispatcher_sends_what_was_committed()
    {
        var transport = new RecordingTransport();
        using var host = Microsoft
            .Extensions.Hosting.Host.CreateDefaultBuilder()
            .ConfigureLogging(logging => logging.ClearProviders())
            .ConfigureServices(services =>
            {
                services.UseSqlServerOutbox(
                    SqlServerSetup.ConnectionString,
                    o =>
                    {
                        o.TableName = $"Outbox_{Guid.NewGuid():N}";
                        o.PollInterval = TimeSpan.FromMilliseconds(50);
                    }
                );
                services.AddSingleton<IOutboxTransport>(transport);
            })
            .Build();
        await host.StartAsync();

        var committed = Message("committed");
        var rolledBack = Message("rolled-back");
        foreach (var (message, commit) in new[] { (committed, true), (rolledBack, false) })
        {
            using var scope = host.Services.CreateScope();
            await using var connection = new SqlConnection(SqlServerSetup.ConnectionString);
            await connection.OpenAsync();
            await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync();
            var session = scope.ServiceProvider.GetRequiredService<IOutboxSession>();
            session.UseSqlTransaction(transaction);
            await session.AddAsync([message], CancellationToken.None);
            if (commit)
                await transaction.CommitAsync();
            else
                await transaction.RollbackAsync();
        }

        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (transport.Sent.IsEmpty && DateTime.UtcNow < deadline)
            await Task.Delay(50);
        //Give a wrongly dispatched message time to show up
        await Task.Delay(300);
        await host.StopAsync();

        transport.Sent.Select(m => m.Id).Should().Equal(committed.Id);
    }

    private async Task<T> ReadAsync<T>(string column, Guid id)
    {
        await using var connection = new SqlConnection(SqlServerSetup.ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand(
            $"SELECT {column} FROM [{_options.Schema}].[{_options.TableName}] WHERE Id = @Id",
            connection
        );
        command.Parameters.AddWithValue("@Id", id);
        var value = await command.ExecuteScalarAsync();
        return value is DBNull ? default! : (T)value!;
    }

    private sealed class RecordingTransport : IOutboxTransport
    {
        public System.Collections.Concurrent.ConcurrentQueue<OutboxMessage> Sent { get; } = new();

        public Task SendAsync(
            IReadOnlyList<OutboxMessage> messages,
            CancellationToken cancellationToken
        )
        {
            foreach (var message in messages)
                Sent.Enqueue(message);
            return Task.CompletedTask;
        }
    }
}
