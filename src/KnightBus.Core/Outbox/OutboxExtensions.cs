using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace KnightBus.Core.Outbox;

public static class OutboxExtensions
{
    /// <summary>
    /// Registers the outbox settings and, unless <see cref="OutboxOptions.EnableDispatcher"/> is
    /// off, the <see cref="OutboxDispatcher"/> as a hosted service. Called by the store packages,
    /// which also register the <see cref="IOutboxSession"/> and <see cref="IOutboxStore"/>. A
    /// transport package registers the <see cref="IOutboxTransport"/>.
    /// </summary>
    public static IServiceCollection AddOutbox(
        this IServiceCollection services,
        OutboxOptions options
    )
    {
        options.Validate();
        services.TryAddSingleton<OutboxOptions>(options);
        if (options.EnableDispatcher)
            services.AddHostedService<OutboxDispatcher>();
        return services;
    }
}
