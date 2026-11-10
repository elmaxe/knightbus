using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using KnightBus.Core.Outbox;
using Microsoft.Data.SqlClient;

namespace KnightBus.SqlServer.Outbox;

/// <summary>
/// Writes outbox messages to SQL Server in the transaction enlisted with
/// <see cref="Enlist"/>, or <see cref="SqlServerOutboxExtensions.UseSqlTransaction"/>.
/// </summary>
public sealed class SqlServerOutboxSession : IOutboxSession
{
    //SQL Server takes at most 2100 parameters per command
    private const int ParametersPerRow = 6;
    private const int MaxRowsPerCommand = 2000 / ParametersPerRow;

    private readonly SqlServerOutboxOptions _options;
    private SqlTransaction? _transaction;

    public SqlServerOutboxSession(SqlServerOutboxOptions options)
    {
        _options = options;
    }

    /// <summary>
    /// Writes messages added from now on in <paramref name="transaction"/>. Enlisting another
    /// transaction later in the same scope replaces it.
    /// </summary>
    public void Enlist(SqlTransaction transaction)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        if (transaction.Connection is null)
            throw new ArgumentException(
                "The transaction has already been committed or rolled back",
                nameof(transaction)
            );
        _transaction = transaction;
    }

    //A committed or rolled back transaction no longer has a connection
    public bool IsEnlisted => _transaction?.Connection != null;

    public async Task AddAsync(
        IReadOnlyList<OutboxMessage> messages,
        CancellationToken cancellationToken
    )
    {
        var transaction = _transaction;
        var connection = transaction?.Connection;
        if (transaction is null || connection is null)
            throw new OutboxNotEnlistedException();

        for (var offset = 0; offset < messages.Count; offset += MaxRowsPerCommand)
        {
            var count = Math.Min(MaxRowsPerCommand, messages.Count - offset);
            await using var command = CreateInsert(
                messages,
                offset,
                count,
                connection,
                transaction
            );
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private SqlCommand CreateInsert(
        IReadOnlyList<OutboxMessage> messages,
        int offset,
        int count,
        SqlConnection connection,
        SqlTransaction transaction
    )
    {
        var command = new SqlCommand { Connection = connection, Transaction = transaction };
        var sql = new StringBuilder(
            $"INSERT INTO {_options.QualifiedTableName} (Id, Destination, Body, ContentType, Properties, ScheduledEnqueueTimeUtc) VALUES "
        );
        for (var i = 0; i < count; i++)
        {
            var message = messages[offset + i];
            if (i > 0)
                sql.Append(',');
            sql.Append(
                $"(@Id{i}, @Destination{i}, @Body{i}, @ContentType{i}, @Properties{i}, @Scheduled{i})"
            );

            command.Parameters.Add($"@Id{i}", SqlDbType.UniqueIdentifier).Value = message.Id;
            command.Parameters.Add($"@Destination{i}", SqlDbType.NVarChar, 260).Value =
                message.Destination;
            command.Parameters.Add($"@Body{i}", SqlDbType.VarBinary, -1).Value = message.Body;
            command.Parameters.Add($"@ContentType{i}", SqlDbType.NVarChar, 256).Value =
                (object?)message.ContentType ?? DBNull.Value;
            command.Parameters.Add($"@Properties{i}", SqlDbType.NVarChar, -1).Value =
                message.Properties.Count == 0
                    ? DBNull.Value
                    : JsonSerializer.Serialize(message.Properties);
            command.Parameters.Add($"@Scheduled{i}", SqlDbType.DateTime2).Value =
                message.ScheduledEnqueueTimeUtc is { } scheduled
                    ? scheduled.UtcDateTime
                    : DBNull.Value;
        }
        command.CommandText = sql.ToString();
        return command;
    }
}
