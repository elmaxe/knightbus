using AwesomeAssertions;
using Azure.Messaging.ServiceBus;
using KnightBus.Core;
using KnightBus.Messages;
using KnightBus.Newtonsoft;
using KnightBus.Shared.Tests.Integration;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;

namespace KnightBus.Azure.ServiceBus.Tests.Integration;

[TestFixture]
public class ServiceBusMessageStateHandlerTests : MessageStateHandlerTests<StateHandlerCommand>
{
    private static readonly string Queue = AutoMessageMapper.GetQueueName<StateHandlerCommand>();
    private ServiceBusClient _client = null!;
    private ServiceBusReceiver _receiver = null!;
    private readonly List<ServiceBusReceivedMessage> _locked = new();

    [OneTimeSetUp]
    public void OneTimeSetUp() => _client = new ServiceBusClient(ServiceBusSetup.ConnectionString);

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _client.DisposeAsync();

    public override async Task Setup()
    {
        await EmulatorEntities.PurgeQueue(_client, Queue);
        _receiver = _client.CreateReceiver(Queue);
    }

    [TearDown]
    public async Task TearDown()
    {
        // Release what the tests left locked so the next purge can remove it
        foreach (var message in _locked)
        {
            try
            {
                await _receiver.AbandonMessageAsync(message);
            }
            catch (ServiceBusException) { }
        }
        _locked.Clear();
        await _receiver.DisposeAsync();
    }

    // The shared complete test receives, which cannot tell a completed message from a locked one
    [Test]
    public async Task Should_complete_the_message_and_not_just_keep_it_locked()
    {
        //arrange
        await SendMessage("Testing Complete");
        var stateHandler = await GetMessageStateHandler();

        //act
        await stateHandler.CompleteAsync();

        //assert
        (await _receiver.PeekMessagesAsync(10, fromSequenceNumber: 0))
            .Should()
            .BeEmpty();
    }

    [Test]
    public async Task Should_schedule_a_copy_and_complete_the_original_when_retrying_with_a_delay()
    {
        //arrange
        await SendMessage("Testing delayed retry");
        var stateHandler = await GetMessageStateHandler(
            deadLetterLimit: 5,
            new FixedRetryDelay(TimeSpan.FromHours(1))
        );
        var original = _locked.Single();

        //act
        await stateHandler.AbandonByErrorAsync(new InvalidOperationException("boom"));

        //assert
        var messages = await _receiver.PeekMessagesAsync(10, fromSequenceNumber: 0);
        messages.Should().ContainSingle();
        var retry = messages[0];
        retry.State.Should().Be(ServiceBusMessageState.Scheduled);
        retry.SequenceNumber.Should().NotBe(original.SequenceNumber);
        retry
            .ScheduledEnqueueTime.Should()
            .BeCloseTo(DateTimeOffset.UtcNow.AddHours(1), TimeSpan.FromMinutes(1));
        retry.MessageId.Should().NotBe(original.MessageId);
        retry
            .ApplicationProperties.Should()
            .Contain(ServiceBusMessageProperties.PreviousDeliveryCount, 1);
        retry
            .ApplicationProperties.Should()
            .Contain(ServiceBusMessageProperties.OriginalMessageId, original.MessageId);
        retry.ApplicationProperties.Should().Contain("ErrorMessage", "boom");
        Deserialize(retry).Message.Should().Be("Testing delayed retry");
    }

    [Test]
    public async Task Should_add_the_deliveries_of_earlier_copies_to_the_delivery_count()
    {
        //arrange
        await SendMessage(
            "Testing delivery count",
            new Dictionary<string, object>
            {
                [ServiceBusMessageProperties.PreviousDeliveryCount] = 3,
                [ServiceBusMessageProperties.OriginalMessageId] = "first",
            }
        );
        var stateHandler = await GetMessageStateHandler(
            deadLetterLimit: 10,
            new FixedRetryDelay(TimeSpan.FromHours(1))
        );

        //act
        await stateHandler.AbandonByErrorAsync(new InvalidOperationException("boom"));

        //assert
        stateHandler.DeliveryCount.Should().Be(4);
        var retry = (await _receiver.PeekMessagesAsync(10, fromSequenceNumber: 0)).Single();
        retry
            .ApplicationProperties.Should()
            .Contain(ServiceBusMessageProperties.PreviousDeliveryCount, 4);
        retry
            .ApplicationProperties.Should()
            .Contain(ServiceBusMessageProperties.OriginalMessageId, "first");
    }

    [Test]
    public async Task Should_abandon_right_away_when_the_next_delivery_dead_letters_the_message()
    {
        //arrange
        await SendMessage("Testing last attempt");
        var stateHandler = await GetMessageStateHandler(
            deadLetterLimit: 1,
            new FixedRetryDelay(TimeSpan.FromHours(1))
        );

        //act
        await stateHandler.AbandonByErrorAsync(new InvalidOperationException("boom"));

        //assert
        await AssertAbandonedRightAway();
    }

    [Test]
    public async Task Should_abandon_right_away_when_processing_was_cancelled()
    {
        //arrange
        await SendMessage("Testing cancellation");
        var stateHandler = await GetMessageStateHandler(
            deadLetterLimit: 5,
            new FixedRetryDelay(TimeSpan.FromHours(1))
        );

        //act
        await stateHandler.AbandonByErrorAsync(new OperationCanceledException());

        //assert
        await AssertAbandonedRightAway();
    }

    [Test]
    public async Task Should_abandon_right_away_when_the_delay_is_zero()
    {
        //arrange
        await SendMessage("Testing zero delay");
        var stateHandler = await GetMessageStateHandler(
            deadLetterLimit: 5,
            new FixedRetryDelay(TimeSpan.Zero)
        );

        //act
        await stateHandler.AbandonByErrorAsync(new InvalidOperationException("boom"));

        //assert
        await AssertAbandonedRightAway();
    }

    private async Task AssertAbandonedRightAway()
    {
        var messages = await _receiver.PeekMessagesAsync(10, fromSequenceNumber: 0);
        messages.Should().ContainSingle();
        messages[0].State.Should().Be(ServiceBusMessageState.Active);
        var redelivered = await _receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        redelivered.Should().NotBeNull();
        _locked.Add(redelivered);
        redelivered.DeliveryCount.Should().Be(2);
    }

    protected override async Task<List<StateHandlerCommand>> GetMessages(int count)
    {
        // Peek also returns locked messages, receiving only returns the visible ones
        var messages = await EmulatorEntities.Receive(_receiver, count);
        foreach (var message in messages)
            await _receiver.AbandonMessageAsync(message);
        return messages.Select(Deserialize).ToList();
    }

    protected override async Task<List<StateHandlerCommand>> GetDeadLetterMessages(int count)
    {
        await using var receiver = _client.CreateReceiver(
            Queue,
            new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter }
        );
        var messages = await receiver.PeekMessagesAsync(count);
        return messages.Select(Deserialize).ToList();
    }

    protected override Task SendMessage(string message) => SendMessage(message, null);

    private async Task SendMessage(string message, IDictionary<string, object>? properties)
    {
        await using var sender = _client.CreateSender(Queue);
        var sbMessage = new ServiceBusMessage(
            new NewtonsoftSerializer().Serialize(new StateHandlerCommand { Message = message })
        );
        foreach (var property in properties ?? new Dictionary<string, object>())
            sbMessage.ApplicationProperties[property.Key] = property.Value;
        await sender.SendMessageAsync(sbMessage);
    }

    protected override Task<IMessageStateHandler<StateHandlerCommand>> GetMessageStateHandler() =>
        GetMessageStateHandler(1, null);

    private async Task<IMessageStateHandler<StateHandlerCommand>> GetMessageStateHandler(
        int deadLetterLimit,
        IServiceBusRetryDelay? retryDelay
    )
    {
        var message = await _receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        _locked.Add(message);
        var delayedRetry = retryDelay is null
            ? null
            : new ServiceBusDelayedRetry(
                _client.CreateSender(Queue),
                retryDelay,
                NullLogger.Instance
            );
        return new ServiceBusMessageStateHandler<StateHandlerCommand>(
            new ProcessMessageEventArgs(message, _receiver, CancellationToken.None),
            new NewtonsoftSerializer(),
            deadLetterLimit,
            null!,
            delayedRetry
        );
    }

    private class FixedRetryDelay(TimeSpan delay) : IServiceBusRetryDelay
    {
        public TimeSpan GetRetryDelay(int deliveryCount) => delay;
    }

    private static StateHandlerCommand Deserialize(ServiceBusReceivedMessage message) =>
        new NewtonsoftSerializer().Deserialize<StateHandlerCommand>(message.Body.ToMemory());
}
