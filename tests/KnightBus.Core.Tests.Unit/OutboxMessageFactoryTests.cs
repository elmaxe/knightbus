using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using AwesomeAssertions;
using KnightBus.Core.Outbox;
using KnightBus.Core.PreProcessors;
using KnightBus.Messages;
using Moq;
using NUnit.Framework;

namespace KnightBus.Core.Tests.Unit;

[TestFixture]
public class OutboxMessageFactoryTests
{
    [Test]
    public async Task Serializes_the_message_with_its_destination_and_pre_processed_properties()
    {
        var serializer = new Mock<IMessageSerializer>();
        serializer.Setup(s => s.Serialize(It.IsAny<TestCommand>())).Returns([42]);
        serializer.Setup(s => s.ContentType).Returns("application/test");
        var preProcessor = new Mock<IMessagePreProcessor>();
        preProcessor
            .Setup(p => p.PreProcess(It.IsAny<TestCommand>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(
                new Dictionary<string, object> { ["traceparent"] = "00-abc-01", ["number"] = 1.5 }
            );
        var scheduled = new DateTimeOffset(2030, 1, 2, 3, 4, 5, TimeSpan.Zero);
        var factory = new OutboxMessageFactory(serializer.Object, [preProcessor.Object]);

        var message = await factory.CreateAsync(
            new TestCommand(),
            scheduled,
            CancellationToken.None
        );

        message.Id.Should().NotBe(Guid.Empty);
        message.Destination.Should().Be("test-command-queue");
        message.Body.Should().Equal(42);
        message.ContentType.Should().Be("application/test");
        message.ScheduledEnqueueTimeUtc.Should().Be(scheduled);
        message
            .Properties.Should()
            .BeEquivalentTo(
                new Dictionary<string, string> { ["traceparent"] = "00-abc-01", ["number"] = "1.5" }
            );
    }

    [Test]
    public async Task Gives_every_message_its_own_id()
    {
        var serializer = new Mock<IMessageSerializer>();
        serializer
            .Setup(s => s.Serialize(It.IsAny<TestCommand>()))
            .Returns(Encoding.UTF8.GetBytes("{}"));
        var factory = new OutboxMessageFactory(serializer.Object, []);

        var first = await factory.CreateAsync(new TestCommand(), null, CancellationToken.None);
        var second = await factory.CreateAsync(new TestCommand(), null, CancellationToken.None);

        first.Id.Should().NotBe(second.Id);
        first.ScheduledEnqueueTimeUtc.Should().BeNull();
    }
}
