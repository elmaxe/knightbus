using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace KnightBus.SqlServer.Outbox;

/// <summary>
/// The table the SQL Server outbox keeps its messages in.
/// </summary>
public static class SqlServerOutboxSchema
{
    /// <summary>
    /// A script that creates the schema and table if they do not exist, for use in your own
    /// migrations when <see cref="SqlServerOutboxOptions.CreateTable"/> is off.
    /// </summary>
    public static string GetCreateScript(SqlServerOutboxOptions options)
    {
        options.Validate();
        var schema = options.Schema;
        var table = options.TableName;
        //Sequence keeps the messages in the order they were written and new rows at the end of
        //the clustered index. Id is the message id the broker sees
        return $"""
            IF SCHEMA_ID(N'{schema}') IS NULL
                EXEC(N'CREATE SCHEMA [{schema}]');

            IF OBJECT_ID(N'[{schema}].[{table}]', N'U') IS NULL
            BEGIN
                CREATE TABLE [{schema}].[{table}]
                (
                    Sequence BIGINT IDENTITY(1, 1) NOT NULL CONSTRAINT [PK_{table}] PRIMARY KEY CLUSTERED,
                    Id UNIQUEIDENTIFIER NOT NULL,
                    Destination NVARCHAR(260) NOT NULL,
                    Body VARBINARY(MAX) NOT NULL,
                    ContentType NVARCHAR(256) NULL,
                    Properties NVARCHAR(MAX) NULL,
                    ScheduledEnqueueTimeUtc DATETIME2 NULL,
                    CreatedUtc DATETIME2 NOT NULL CONSTRAINT [DF_{table}_CreatedUtc] DEFAULT SYSUTCDATETIME(),
                    Attempts INT NOT NULL CONSTRAINT [DF_{table}_Attempts] DEFAULT 0,
                    LockedUntilUtc DATETIME2 NULL,
                    LockToken UNIQUEIDENTIFIER NULL,
                    FailedUtc DATETIME2 NULL,
                    LastError NVARCHAR(MAX) NULL
                );
                CREATE UNIQUE NONCLUSTERED INDEX [UX_{table}_Id] ON [{schema}].[{table}] (Id);
            END
            """;
    }

    /// <summary>
    /// Creates the schema and table if they do not exist.
    /// </summary>
    public static async Task EnsureCreatedAsync(
        string connectionString,
        SqlServerOutboxOptions options,
        CancellationToken cancellationToken = default
    )
    {
        var script = GetCreateScript(options);
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new SqlCommand(script, connection);
        try
        {
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        //Another instance created it at the same time
        catch (SqlException e) when (e.Number is 2714 or 1913) { }
    }
}
