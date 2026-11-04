using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using KnightBus.Core;
using KnightBus.Messages;

[assembly: InternalsVisibleTo("BB.Common.KnightBus.Tests.Unit")]
[assembly: InternalsVisibleTo("KnightBus.Azure.ServiceBus.Tests.Integration")]

namespace KnightBus.Azure.ServiceBus;

internal class ServiceBusQueueChannelReceiver<T> : ServiceBusChannelReceiverBase<T>
    where T : class, ICommand
{
    public ServiceBusQueueChannelReceiver(
        IProcessingSettings settings,
        IMessageSerializer serializer,
        IServiceBusConfiguration configuration,
        IHostConfiguration hostConfiguration,
        IMessageProcessor processor
    )
        : base(settings, serializer, configuration, hostConfiguration, processor) { }

    protected override Task<ServiceBusProcessor> CreateClient(CancellationToken cancellationToken)
    {
        return ClientFactory.GetReceiverClient<T>(
            new ServiceBusProcessorOptions
            {
                AutoCompleteMessages = false,
                MaxAutoLockRenewalDuration = Settings.MessageLockTimeout,
                MaxConcurrentCalls = Settings.MaxConcurrentCalls,
                PrefetchCount = Settings.PrefetchCount,
                ReceiveMode = ServiceBusReceiveMode.PeekLock,
            }
        );
    }

    protected override Task CreateMessagingEntity(CancellationToken cancellationToken)
    {
        return ServiceBusEntities.CreateQueue(
            ManagementClient,
            AutoMessageMapper.GetQueueName<T>(),
            GetServiceBusCreationOptions(),
            Log,
            cancellationToken
        );
    }
}
