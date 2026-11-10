using AwesomeAssertions;
using Azure.Messaging.ServiceBus;
using KnightBus.Core;
using KnightBus.Core.Outbox;
using KnightBus.Core.PreProcessors;
using KnightBus.Messages;
using KnightBus.Newtonsoft;
using NUnit.Framework;

namespace KnightBus.Azure.ServiceBus.Tests.Integration;

[TestFixture]
public class ServiceBusOutboxTests
{
    private static readonly string Queue = AutoMessageMapper.GetQueueName<OutboxCommand>();
    private static readonly string Topic = AutoMessageMapper.GetQueueName<OutboxEvent>();
    private const string Subscription = "outbox-sub";

    private static readonly ServiceBusReceiverOptions ReceiveAndDelete = new()
    {
        ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete,
    };
    private ServiceBusClient _client = null!;
    private ServiceBusOutboxTransport _transport = null!;
    private InMemorySession _session = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _client = new ServiceBusClient(ServiceBusSetup.ConnectionString);
        _transport = new ServiceBusOutboxTransport(
            new ServiceBusConfiguration(ServiceBusSetup.ConnectionString)
        );
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _transport.DisposeAsync();
        await _client.DisposeAsync();
    }

    [SetUp]
    public async Task SetUp()
    {
        _session = new InMemorySession();
        await EmulatorEntities.PurgeQueue(_client, Queue);
        await EmulatorEntities.PurgeSubscription(_client, Topic, Subscription);
    }

    private ServiceBusOutbox CreateOutbox(params IMessagePreProcessor[] preProcessors) =>
        new(_session, new ServiceBusConfiguration(ServiceBusSetup.ConnectionString), preProcessors);

    private Task DispatchAsync() =>
        Task.WhenAll(
            _session
                .Added.GroupBy(m => m.Destination)
                .Select(g => _transport.SendAsync(g.ToList(), CancellationToken.None))
        );

    [Test]
    public async Task A_command_written_to_the_outbox_arrives_as_if_sent_directly()
    {
        await CreateOutbox(new StaticPropertiesPreProcessor())
            .SendAsync(new OutboxCommand { Message = "one" });
        await DispatchAsync();

        var received = await Receive(1);
        var message = received.Should().ContainSingle().Subject;
        Deserialize<OutboxCommand>(message).Message.Should().Be("one");
        message.MessageId.Should().Be(_session.Added[0].Id.ToString());
        message.ContentType.Should().Be(new NewtonsoftSerializer().ContentType);
        message.ApplicationProperties.Should().Contain("Origin", "outbox-tests");
    }

    [Test]
    public async Task Events_are_published_to_the_topic()
    {
        await CreateOutbox()
            .PublishEventsAsync(
                Enumerable.Range(0, 20).Select(i => new OutboxEvent { Message = $"{i}" })
            );
        await DispatchAsync();

        await using var receiver = _client.CreateReceiver(Topic, Subscription, ReceiveAndDelete);
        var received = await EmulatorEntities.Receive(receiver, 20, TimeSpan.FromSeconds(5));
        received
            .Select(m => Deserialize<OutboxEvent>(m).Message)
            .Should()
            .BeEquivalentTo(Enumerable.Range(0, 20).Select(i => $"{i}"));
    }

    [Test]
    public async Task A_scheduled_command_is_not_delivered_before_its_time()
    {
        await CreateOutbox()
            .ScheduleAsync(new OutboxCommand { Message = "later" }, TimeSpan.FromSeconds(3));
        await DispatchAsync();

        (await Receive(1, TimeSpan.FromMilliseconds(500))).Should().BeEmpty();
        var received = await Receive(1, TimeSpan.FromSeconds(10));
        received.Select(m => Deserialize<OutboxCommand>(m).Message).Should().Equal("later");
    }

    [Test]
    public async Task A_message_dispatched_twice_is_delivered_once_with_duplicate_detection()
    {
        await CreateOutbox().SendAsync(new OutboxCommand { Message = "once" });

        await DispatchAsync();
        await DispatchAsync();

        var received = await Receive(2, TimeSpan.FromSeconds(2));
        received.Select(m => Deserialize<OutboxCommand>(m).Message).Should().Equal("once");
    }

    [Test]
    public async Task Refuses_messages_without_an_enlisted_transaction_before_pre_processing()
    {
        _session.IsEnlisted = false;
        var preProcessor = new StaticPropertiesPreProcessor();

        var act = () => CreateOutbox(preProcessor).SendAsync(new OutboxCommand());

        await act.Should().ThrowAsync<OutboxNotEnlistedException>();
        preProcessor.Calls.Should().Be(0);
        _session.Added.Should().BeEmpty();
    }

    private async Task<IReadOnlyList<ServiceBusReceivedMessage>> Receive(
        int count,
        TimeSpan? maxWait = null
    )
    {
        await using var receiver = _client.CreateReceiver(Queue, ReceiveAndDelete);
        return await EmulatorEntities.Receive(receiver, count, maxWait ?? TimeSpan.FromSeconds(5));
    }

    private static T Deserialize<T>(ServiceBusReceivedMessage message) =>
        new NewtonsoftSerializer().Deserialize<T>(message.Body.ToMemory());

    private sealed class InMemorySession : IOutboxSession
    {
        public List<OutboxMessage> Added { get; } = new();
        public bool IsEnlisted { get; set; } = true;

        public Task AddAsync(
            IReadOnlyList<OutboxMessage> messages,
            CancellationToken cancellationToken
        )
        {
            Added.AddRange(messages);
            return Task.CompletedTask;
        }
    }

    private sealed class StaticPropertiesPreProcessor : IMessagePreProcessor
    {
        public int Calls { get; private set; }

        public Task<IDictionary<string, object>> PreProcess<T>(
            T message,
            CancellationToken cancellationToken
        )
            where T : IMessage
        {
            Calls++;
            return Task.FromResult<IDictionary<string, object>>(
                new Dictionary<string, object> { ["Origin"] = "outbox-tests" }
            );
        }
    }
}
