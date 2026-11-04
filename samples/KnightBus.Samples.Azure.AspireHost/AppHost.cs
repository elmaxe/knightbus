var builder = DistributedApplication.CreateBuilder(args);

// Add Azure Service Bus (will use emulator for local development)
var serviceBus = builder.AddAzureServiceBus("servicebus").RunAsEmulator();

// Add queues
serviceBus.AddServiceBusQueue("your-queue");
serviceBus.AddServiceBusQueue("other-queue");

// Add topic with subscriptions. KnightBus forwards each subscription to a queue of its own and
// receives from that queue, and the emulator cannot create them at runtime, so both are declared
var topic = serviceBus.AddServiceBusTopic("your-topic");
foreach (var subscription in new[] { "subscription-1", "subscription-2" })
{
    var queueName = $"your-topic.{subscription}";
    serviceBus.AddServiceBusQueue($"{subscription}-queue", queueName);
    topic.AddServiceBusSubscription(subscription).WithProperties(s => s.ForwardTo = queueName);
}

// Add the consumer (KnightBus host) - waits for Service Bus to be ready
builder
    .AddProject<Projects.KnightBus_Samples_Azure_ServiceBus>("servicebushost")
    .WithReference(serviceBus)
    .WaitFor(serviceBus);

// Add the producer - waits for Service Bus to be ready
builder
    .AddProject<Projects.KnightBus_Samples_Azure_ServiceBus_Producer>("producer")
    .WithReference(serviceBus)
    .WaitFor(serviceBus);

builder.Build().Run();
