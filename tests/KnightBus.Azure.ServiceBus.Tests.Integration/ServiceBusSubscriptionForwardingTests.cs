using AwesomeAssertions;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using KnightBus.Azure.ServiceBus.Management;
using KnightBus.Core;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;

namespace KnightBus.Azure.ServiceBus.Tests.Integration;

[TestFixture]
public class ServiceBusSubscriptionForwardingTests
{
    private static readonly string Topic = AutoMessageMapper.GetQueueName<ForwardingEvent>();
    private static readonly string SubscriptionA = new ForwardingEventSubscriptionA().Name;
    private const string SubscriptionB = "forwarding-b";
    private static readonly string QueueA = SubscriptionForwarding.QueueName(Topic, SubscriptionA);
    private static readonly string QueueB = SubscriptionForwarding.QueueName(Topic, SubscriptionB);
    private static readonly ServiceBusReceiverOptions ReceiveAndDelete = new()
    {
        ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete,
    };
    private ServiceBusClient _client = null!;
    private ServiceBusAdministrationClient _adminClient = null!;
    private ServiceBusConfiguration _configuration = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _client = new ServiceBusClient(ServiceBusSetup.ConnectionString);
        _adminClient = new ServiceBusAdministrationClient(
            ServiceBusSetup.AdministrationConnectionString
        );
        _configuration = new ServiceBusConfiguration(ServiceBusSetup.ConnectionString);
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _client.DisposeAsync();

    [SetUp]
    public async Task SetUp()
    {
        await EmulatorEntities.PurgeForwardedSubscription(_client, Topic, SubscriptionA);
        await EmulatorEntities.PurgeForwardedSubscription(_client, Topic, SubscriptionB);
    }

    [Test]
    public void Should_name_the_queue_after_the_topic_and_the_subscription() =>
        SubscriptionForwarding.QueueName("orders", "billing").Should().Be("orders.billing");

    [TestCase(null, null)]
    [TestCase("", null)]
    [TestCase("orders.billing", "orders.billing")]
    [TestCase("sb://ns.servicebus.windows.net/orders.billing", "orders.billing")]
    [TestCase("sb://ns.servicebus.windows.net/orders.billing/", "orders.billing")]
    public void Should_read_the_entity_name_from_a_forward_to_address(
        string? forwardTo,
        string? expected
    ) => SubscriptionForwarding.EntityName(forwardTo).Should().Be(expected);

    [Test]
    public async Task Should_receive_a_subscription_from_its_forwarding_queue_by_default()
    {
        //arrange
        await using var factory = new ClientFactory(_configuration);

        //act
        await using var processor = await factory.GetReceiverClient<
            ForwardingEvent,
            ForwardingEventSubscriptionA
        >(new ForwardingEventSubscriptionA(), new ServiceBusProcessorOptions());

        //assert
        processor.EntityPath.Should().Be(QueueA);
    }

    [Test]
    public async Task Should_receive_from_the_subscription_when_forwarding_is_disabled_on_the_configuration()
    {
        //arrange
        var configuration = new ServiceBusConfiguration(ServiceBusSetup.ConnectionString);
        configuration.DefaultCreationOptions.ForwardSubscriptionsToQueue = false;
        await using var factory = new ClientFactory(configuration);

        //act
        await using var processor = await factory.GetReceiverClient<
            ForwardingEvent,
            ForwardingEventSubscriptionA
        >(new ForwardingEventSubscriptionA(), new ServiceBusProcessorOptions());

        //assert
        processor.EntityPath.Should().Be($"{Topic}/Subscriptions/{SubscriptionA}");
    }

    [Test]
    public async Task Should_receive_from_the_subscription_when_the_mapping_disables_forwarding()
    {
        //arrange
        await using var factory = new ClientFactory(_configuration);

        //act
        await using var processor = await factory.GetReceiverClient<
            NotForwardingEvent,
            NotForwardingEventSubscription
        >(new NotForwardingEventSubscription(), new ServiceBusProcessorOptions());

        //assert
        processor.EntityPath.Should().Be("not-forwarding-events/Subscriptions/not-forwarding-sub");
    }

    [Test]
    public async Task Should_deliver_an_event_to_the_queue_of_every_forwarding_subscription()
    {
        //act
        await Publish("{\"n\":1}");

        //assert
        (await ReceiveEventually(QueueA))
            .Select(Body)
            .Should()
            .Equal("{\"n\":1}");
        (await ReceiveEventually(QueueB)).Select(Body).Should().Equal("{\"n\":1}");
    }

    [Test]
    public async Task Should_move_dead_letters_back_to_their_subscription_only()
    {
        //arrange: both subscriptions got the event, only A failed it
        await Publish("{\"n\":1}");
        await DeadLetterFromQueue(QueueA);
        (await ReceiveEventually(QueueB)).Should().ContainSingle();
        await using var queueManager = new ServiceBusQueueManager(_configuration);

        //act
        var moved = await queueManager.MoveDeadLetters(QueueA, 1, CancellationToken.None);

        //assert
        moved.Should().Be(1);
        (await ReceiveEventually(QueueA)).Select(Body).Should().Equal("{\"n\":1}");
        (await ReceiveAll(QueueB))
            .Should()
            .BeEmpty("B handled the event already and must not get it again");
    }

    // The emulator fails every read of a subscription for minutes after its forwarding was changed,
    // so this test makes a subscription of its own instead of changing one from Config.json
    [Test]
    public async Task Should_point_an_existing_subscription_at_its_queue_when_the_entities_are_created()
    {
        //arrange: a subscription created before forwarding, with a message waiting on it
        const string subscription = "forwarding-migrated";
        var queue = SubscriptionForwarding.QueueName(Topic, subscription);
        await _adminClient.CreateSubscriptionAsync(Topic, subscription);
        try
        {
            await Publish("{\"n\":1}");

            //act
            await CreateEntities(subscription);
            await Publish("{\"n\":2}");

            //assert
            SubscriptionProperties properties = await _adminClient.GetSubscriptionAsync(
                Topic,
                subscription
            );
            SubscriptionForwarding.EntityName(properties.ForwardTo).Should().Be(queue);
            var received = new List<ServiceBusReceivedMessage>();
            await using var receiver = _client.CreateReceiver(queue, ReceiveAndDelete);
            await EmulatorEntities.Eventually(
                async () =>
                {
                    received.AddRange(await EmulatorEntities.Receive(receiver, 10));
                    return received.Count;
                },
                count => count >= 2
            );
            received.Select(Body).Should().BeEquivalentTo("{\"n\":1}", "{\"n\":2}");
        }
        finally
        {
            await _adminClient.DeleteSubscriptionAsync(Topic, subscription);
            if (await _adminClient.QueueExistsAsync(queue))
                await _adminClient.DeleteQueueAsync(queue);
        }
    }

    [Test]
    public async Task Should_leave_a_subscription_that_already_forwards_to_its_queue_as_it_is()
    {
        //act
        await CreateEntities(SubscriptionA);

        //assert
        SubscriptionProperties subscription = await _adminClient.GetSubscriptionAsync(
            Topic,
            SubscriptionA
        );
        SubscriptionForwarding.EntityName(subscription.ForwardTo).Should().Be(QueueA);
    }

    // Not covered here: the subscription manager moving a forwarding subscription's own dead letters
    // to its queue. The emulator fails reads of a subscription's dead letter queue for minutes after
    // the subscription was changed to forward, and nothing can be dead-lettered on a subscription
    // that forwards from the start. EntityName, which picks the destination, is covered above
    [Test]
    public async Task Should_delete_the_forwarding_queue_with_its_subscription()
    {
        //arrange
        const string subscription = "forwarding-deleted";
        var queue = SubscriptionForwarding.QueueName(Topic, subscription);
        await CreateEntities(subscription);
        var subscriptionManager = new ServiceBusSubscriptionManager(Topic, _client, _adminClient);

        //act
        await subscriptionManager.Delete(subscription, CancellationToken.None);

        //assert: a receiver waiting on the queue would otherwise never see the subscription go
        (await _adminClient.SubscriptionExistsAsync(Topic, subscription))
            .Value.Should()
            .BeFalse();
        (await _adminClient.QueueExistsAsync(queue)).Value.Should().BeFalse();
    }

    private Task CreateEntities(string subscription) =>
        ServiceBusEntities.CreateSubscription(
            _adminClient,
            Topic,
            subscription,
            _configuration.DefaultCreationOptions,
            NullLogger.Instance,
            CancellationToken.None
        );

    private async Task Publish(string body)
    {
        await using var sender = _client.CreateSender(Topic);
        await sender.SendMessageAsync(new ServiceBusMessage(body));
    }

    private async Task DeadLetterFromQueue(string queue)
    {
        await using var receiver = _client.CreateReceiver(queue);
        var message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        message.Should().NotBeNull();
        await receiver.DeadLetterMessageAsync(message);
    }

    // A message forwarded to a queue arrives there a moment later
    private async Task<IReadOnlyList<ServiceBusReceivedMessage>> ReceiveEventually(string queue)
    {
        await using var receiver = _client.CreateReceiver(queue, ReceiveAndDelete);
        return await EmulatorEntities.Eventually(
            () => EmulatorEntities.Receive(receiver, 10),
            m => m.Count > 0
        );
    }

    // Waits long enough for a forwarded message to have arrived, for asserting that none did
    private async Task<IReadOnlyList<ServiceBusReceivedMessage>> ReceiveAll(string queue)
    {
        await using var receiver = _client.CreateReceiver(queue);
        return await EmulatorEntities.Receive(receiver, 10, TimeSpan.FromSeconds(2));
    }

    private static string Body(ServiceBusReceivedMessage message) => message.Body.ToString();
}
