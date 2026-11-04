using KnightBus.Azure.ServiceBus.Messages;
using KnightBus.Messages;

namespace KnightBus.Azure.ServiceBus.Tests.Integration;

// Queue and topic names must match the entities declared in Config.json, as must the queues
// that subscriptions forward to

public class StateHandlerCommand : IServiceBusCommand
{
    public string? Message { get; set; }
}

public class StateHandlerCommandMapping : IMessageMapping<StateHandlerCommand>
{
    public string QueueName => "state-handler";
}

public class BusCommand : IServiceBusCommand
{
    public string? Message { get; set; }
}

public class BusCommandMapping : IMessageMapping<BusCommand>
{
    public string QueueName => "bus-commands";
}

public class BusEvent : IServiceBusEvent
{
    public string? Message { get; set; }
}

public class BusEventMapping : IMessageMapping<BusEvent>
{
    public string QueueName => "bus-events";
}

public class HostCommand : IServiceBusCommand
{
    public string? Message { get; set; }
}

public class HostCommandMapping : IMessageMapping<HostCommand>
{
    public string QueueName => "host-commands";
}

public class FailingHostCommand : IServiceBusCommand { }

public class FailingHostCommandMapping : IMessageMapping<FailingHostCommand>
{
    public string QueueName => "host-failing";
}

public class HostEvent : IServiceBusEvent
{
    public string? Message { get; set; }
}

public class HostEventMapping : IMessageMapping<HostEvent>
{
    public string QueueName => "host-events";
}

public class HostEventSubscription : IEventSubscription<HostEvent>
{
    public string Name => "host-sub";
}

public class DrainCommand : IServiceBusCommand
{
    public string? Message { get; set; }
}

public class DrainCommandMapping : IMessageMapping<DrainCommand>
{
    public string QueueName => "drain-commands";
}

//On a queue with a short message lock, so a lost lock shows within seconds
public class DrainLockCommand : IServiceBusCommand
{
    public string? Message { get; set; }
}

public class DrainLockCommandMapping : IMessageMapping<DrainLockCommand>
{
    public string QueueName => "drain-lock-commands";
}

public class DrainEvent : IServiceBusEvent
{
    public string? Message { get; set; }
}

public class DrainEventMapping : IMessageMapping<DrainEvent>
{
    public string QueueName => "drain-events";
}

public class DrainEventSubscription : IEventSubscription<DrainEvent>
{
    public string Name => "drain-sub";
}

public class ForwardingEvent : IServiceBusEvent
{
    public string? Message { get; set; }
}

public class ForwardingEventMapping : IMessageMapping<ForwardingEvent>
{
    public string QueueName => "forwarding-events";
}

public class ForwardingEventSubscriptionA : IEventSubscription<ForwardingEvent>
{
    public string Name => "forwarding-a";
}

public class NotForwardingEventMapping
    : IMessageMapping<NotForwardingEvent>,
        IServiceBusCreationOptions
{
    public string QueueName => "not-forwarding-events";
    public bool EnablePartitioning => false;
    public bool SupportOrdering => false;
    public bool EnableBatchedOperations => true;
    public bool ForwardSubscriptionsToQueue => false;
}

public class NotForwardingEvent : IServiceBusEvent { }

public class NotForwardingEventSubscription : IEventSubscription<NotForwardingEvent>
{
    public string Name => "not-forwarding-sub";
}
