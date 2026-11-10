using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using KnightBus.Core.Outbox;
using Microsoft.Data.SqlClient;

namespace KnightBus.SqlServer.Outbox;

/// <summary>
/// The SQL Server side of the outbox dispatcher. Locks are rows updates taken with
/// <c>UPDLOCK, READPAST</c>, so dispatchers skip each other's rows, and rows still being written in an
/// open transaction, instead of waiting for them.
/// </summary>
public sealed class SqlServerOutboxStore : IOutboxStore
{
    private const int MaxErrorLength = 4000;
    private const int MaxIdsPerCommand = 1000;

    private readonly string _connectionString;
    private readonly SqlServerOutboxOptions _options;
    private readonly string _lockSql;

    public SqlServerOutboxStore(string connectionString, SqlServerOutboxOptions options)
    {
        options.Validate();
        _connectionString = connectionString;
        _options = options;
        var table = options.QualifiedTableName;
        _lockSql = $"""
            WITH due AS (
                SELECT TOP (@BatchSize) *
                FROM {table} WITH (ROWLOCK, UPDLOCK, READPAST)
                WHERE FailedUtc IS NULL
                  AND (LockedUntilUtc IS NULL OR LockedUntilUtc <= SYSUTCDATETIME())
                ORDER BY Sequence
            )
            UPDATE due
            SET LockToken = @LockToken,
                LockedUntilUtc = DATEADD(MILLISECOND, @LockMilliseconds, SYSUTCDATETIME()),
                Attempts = Attempts + 1
            OUTPUT inserted.Sequence, inserted.Id, inserted.Destination, inserted.Body,
                   inserted.ContentType, inserted.Properties, inserted.ScheduledEnqueueTimeUtc,
                   inserted.Attempts;
            """;
    }

    public async Task<IReadOnlyList<OutboxRecord>> LockAsync(
        int maxMessages,
        TimeSpan lockDuration,
        CancellationToken cancellationToken
    )
    {
        var lockToken = Guid.NewGuid();
        await using var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new SqlCommand(_lockSql, connection);
        command.Parameters.Add("@BatchSize", SqlDbType.Int).Value = maxMessages;
        command.Parameters.Add("@LockToken", SqlDbType.UniqueIdentifier).Value = lockToken;
        command.Parameters.Add("@LockMilliseconds", SqlDbType.Int).Value = ToMilliseconds(
            lockDuration
        );

        var locked = new List<(long Sequence, OutboxRecord Record)>();
        await using var reader = await command
            .ExecuteReaderAsync(cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var properties = reader.IsDBNull(5)
                ? null
                : JsonSerializer.Deserialize<Dictionary<string, string>>(reader.GetString(5));
            var message = new OutboxMessage
            {
                Id = reader.GetGuid(1),
                Destination = reader.GetString(2),
                Body = (byte[])reader.GetValue(3),
                ContentType = reader.IsDBNull(4) ? null : reader.GetString(4),
                Properties = properties ?? new Dictionary<string, string>(),
                ScheduledEnqueueTimeUtc = reader.IsDBNull(6)
                    ? null
                    : new DateTimeOffset(
                        DateTime.SpecifyKind(reader.GetDateTime(6), DateTimeKind.Utc)
                    ),
            };
            locked.Add(
                (reader.GetInt64(0), new OutboxRecord(message, reader.GetInt32(7), lockToken))
            );
        }

        //OUTPUT does not keep the order of the select
        return locked.OrderBy(l => l.Sequence).Select(l => l.Record).ToList();
    }

    public async Task CompleteAsync(
        IReadOnlyList<OutboxRecord> records,
        CancellationToken cancellationToken
    )
    {
        if (records.Count == 0)
            return;

        await using var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        foreach (var byLock in records.GroupBy(r => r.LockToken))
        {
            foreach (var chunk in byLock.Chunk(MaxIdsPerCommand))
            {
                await using var command = new SqlCommand { Connection = connection };
                command.Parameters.Add("@LockToken", SqlDbType.UniqueIdentifier).Value = byLock.Key;
                var ids = new StringBuilder();
                for (var i = 0; i < chunk.Length; i++)
                {
                    if (i > 0)
                        ids.Append(',');
                    ids.Append($"@Id{i}");
                    command.Parameters.Add($"@Id{i}", SqlDbType.UniqueIdentifier).Value = chunk[i]
                        .Message
                        .Id;
                }
                command.CommandText =
                    $"DELETE FROM {_options.QualifiedTableName} WHERE LockToken = @LockToken AND Id IN ({ids})";
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }
    }

    public async Task RetryLaterAsync(
        OutboxRecord record,
        Exception error,
        TimeSpan delay,
        CancellationToken cancellationToken
    )
    {
        await using var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new SqlCommand(
            $"""
            UPDATE {_options.QualifiedTableName}
            SET LockToken = NULL,
                LockedUntilUtc = DATEADD(MILLISECOND, @DelayMilliseconds, SYSUTCDATETIME()),
                LastError = @LastError
            WHERE Id = @Id AND LockToken = @LockToken
            """,
            connection
        );
        AddRecordParameters(command, record, error);
        command.Parameters.Add("@DelayMilliseconds", SqlDbType.Int).Value = ToMilliseconds(delay);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task FailAsync(
        OutboxRecord record,
        Exception error,
        CancellationToken cancellationToken
    )
    {
        await using var connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new SqlCommand(
            $"""
            UPDATE {_options.QualifiedTableName}
            SET LockToken = NULL,
                LockedUntilUtc = NULL,
                FailedUtc = SYSUTCDATETIME(),
                LastError = @LastError
            WHERE Id = @Id AND LockToken = @LockToken
            """,
            connection
        );
        AddRecordParameters(command, record, error);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static void AddRecordParameters(
        SqlCommand command,
        OutboxRecord record,
        Exception error
    )
    {
        var text = error.ToString();
        if (text.Length > MaxErrorLength)
            text = text[..MaxErrorLength];
        command.Parameters.Add("@Id", SqlDbType.UniqueIdentifier).Value = record.Message.Id;
        command.Parameters.Add("@LockToken", SqlDbType.UniqueIdentifier).Value = record.LockToken;
        command.Parameters.Add("@LastError", SqlDbType.NVarChar, -1).Value = text;
    }

    private static int ToMilliseconds(TimeSpan duration) =>
        (int)Math.Clamp(duration.TotalMilliseconds, 0, int.MaxValue);

    private async Task<SqlConnection> OpenAsync(CancellationToken cancellationToken)
    {
        var connection = new SqlConnection(_connectionString);
        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            return connection;
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }
}
