using System;
using System.Threading;
using System.Threading.Tasks;
using KnightBus.Core.Outbox;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace KnightBus.SqlServer.Outbox;

public static class SqlServerOutboxExtensions
{
    /// <summary>
    /// Keeps the outbox in SQL Server: registers the scoped <see cref="IOutboxSession"/> that writes
    /// to it, the <see cref="IOutboxStore"/> the dispatcher reads from and, unless turned off in
    /// the options, the dispatcher and the creation of the table at startup. A transport is
    /// registered separately, such as <c>UseServiceBusOutbox</c> from KnightBus.Azure.ServiceBus.
    /// </summary>
    public static IServiceCollection UseSqlServerOutbox(
        this IServiceCollection services,
        string connectionString,
        Action<SqlServerOutboxOptions>? configure = null
    )
    {
        var options = new SqlServerOutboxOptions();
        configure?.Invoke(options);
        options.Validate();

        var store = new SqlServerOutboxStore(connectionString, options);
        services.AddSingleton(options);
        services.AddSingleton(store);
        services.AddSingleton<IOutboxStore>(store);
        services.AddScoped<SqlServerOutboxSession>();
        services.AddScoped<IOutboxSession>(provider =>
            provider.GetRequiredService<SqlServerOutboxSession>()
        );

        //Hosted services start in the order they are registered, so the table exists before the
        //dispatcher looks in it
        if (options.CreateTable)
            services.AddHostedService(_ => new SqlServerOutboxInitializer(
                connectionString,
                options
            ));
        services.AddOutbox(options);
        return services;
    }

    /// <summary>
    /// Writes messages added to the outbox in this scope in <paramref name="transaction"/>, so they
    /// are only dispatched if it commits. With Entity Framework Core, pass
    /// <c>(SqlTransaction)dbContext.Database.CurrentTransaction.GetDbTransaction()</c>.
    /// </summary>
    public static IOutboxSession UseSqlTransaction(
        this IOutboxSession session,
        SqlTransaction transaction
    )
    {
        if (session is not SqlServerOutboxSession sqlSession)
            throw new InvalidOperationException(
                $"The outbox session is a {session.GetType().Name}, not a {nameof(SqlServerOutboxSession)}. Register the outbox with {nameof(UseSqlServerOutbox)}"
            );
        sqlSession.Enlist(transaction);
        return session;
    }

    private sealed class SqlServerOutboxInitializer : IHostedService
    {
        private readonly string _connectionString;
        private readonly SqlServerOutboxOptions _options;

        public SqlServerOutboxInitializer(string connectionString, SqlServerOutboxOptions options)
        {
            _connectionString = connectionString;
            _options = options;
        }

        public Task StartAsync(CancellationToken cancellationToken) =>
            SqlServerOutboxSchema.EnsureCreatedAsync(
                _connectionString,
                _options,
                cancellationToken
            );

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
