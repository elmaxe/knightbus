using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using KnightBus.Core;
using KnightBus.Messages;

namespace KnightBus.Azure.ServiceBus;

internal class ServiceBusTopicChannelReceiver<T>
    : ServiceBusChannelReceiverBase<T>,
        IChannelReceiver
    where T : class, IEvent
{
    private readonly IEventSubscription<T> _subscription;

    public ServiceBusTopicChannelReceiver(
        IProcessingSettings settings,
        IMessageSerializer serializer,
        IEventSubscription<T> subscription,
        IServiceBusConfiguration configuration,
        IHostConfiguration hostConfiguration,
        IMessageProcessor processor
    )
        : base(settings, serializer, configuration, hostConfiguration, processor)
    {
        _subscription = subscription;
    }

    //Receives from the subscription's forwarding queue rather than the subscription itself when
    //forwarding is enabled, see ClientFactory. The queue is missing until CreateMessagingEntity
    //runs, which also points a subscription created before forwarding at the queue
    protected override Task<ServiceBusProcessor> CreateClient(CancellationToken cancellationToken)
    {
        return ClientFactory.GetReceiverClient<T, IEventSubscription<T>>(
            _subscription,
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
        return ServiceBusEntities.CreateSubscription(
            ManagementClient,
            AutoMessageMapper.GetQueueName<T>(),
            _subscription.Name,
            GetServiceBusCreationOptions(),
            Log,
            cancellationToken
        );
    }
}
