using System;
using KnightBus.Core.Outbox;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace KnightBus.Azure.ServiceBus;

public static class ServiceBusExtensions
{
    public static IServiceCollection UseServiceBus(
        this IServiceCollection collection,
        Action<IServiceBusConfiguration>? config = null
    )
    {
        var configuration = new ServiceBusConfiguration();
        config?.Invoke(configuration);
        return collection.UseServiceBus(configuration);
    }

    public static IServiceCollection UseServiceBus(
        this IServiceCollection collection,
        IServiceBusConfiguration configuration
    )
    {
        collection.AddSingleton<IServiceBusConfiguration>(configuration);
        collection.AddSingleton<IClientFactory, ClientFactory>();
        collection.AddScoped<IServiceBus, ServiceBus>();
        return collection;
    }

    /// <summary>
    /// Registers <see cref="IServiceBusOutbox"/>, which writes messages to the outbox, and the
    /// transport the outbox dispatcher sends them with. Call <see cref="UseServiceBus(IServiceCollection, IServiceBusConfiguration)"/>
    /// first, and register an outbox store, such as <c>UseSqlServerOutbox</c> from KnightBus.SqlServer.
    /// </summary>
    public static IServiceCollection UseServiceBusOutbox(this IServiceCollection collection)
    {
        collection.TryAddSingleton<IOutboxTransport, ServiceBusOutboxTransport>();
        collection.TryAddScoped<IServiceBusOutbox, ServiceBusOutbox>();
        return collection;
    }
}
