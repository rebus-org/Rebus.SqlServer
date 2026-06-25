using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Config.Outbox;
using Rebus.Routing;
using Rebus.Routing.TypeBased;
using Rebus.Tests.Contracts;
using Rebus.Transport;
using Rebus.Transport.InMem;

// ReSharper disable ArgumentsStyleLiteral
// ReSharper disable AccessToDisposedClosure
#pragma warning disable CS1998

namespace Rebus.SqlServer.Tests.Outbox;

[TestFixture]
public class TestOutbox_Options : FixtureBase
{
    static string ConnectionString => SqlTestHelper.ConnectionString;

    InMemNetwork _network;

    protected override void SetUp()
    {
        base.SetUp();
        SqlTestHelper.DropTable("RebusOutbox");
        _network = new InMemNetwork();
    }

    record SomeMessage;

    [Test]
    public void DefaultOptionsAreCorrect()
    {
        var options = new OutboxOptions();
        Assert.That(options.ForwarderIntervalSeconds, Is.EqualTo(1));
        Assert.That(options.RunForwarder, Is.True);
    }

    [Test]
    public void ThrowsWhenForwarderIntervalIsLessThanOneSecond()
    {
        var options = new OutboxOptions();
        Assert.Throws<ArgumentOutOfRangeException>(() => options.ForwarderIntervalSeconds = 0);
        Assert.Throws<ArgumentOutOfRangeException>(() => options.ForwarderIntervalSeconds = -1);
    }

    [Test]
    public async Task DoesNotForwardMessagesWhenOutboxForwarderIsDisabled()
    {
        var settings = new FlakySenderTransportDecoratorSettings();

        using var messageWasReceived = new ManualResetEvent(initialState: false);
        using var server = CreateConsumer("server", a => a.Handle<SomeMessage>(async _ => messageWasReceived.Set()));
        using var client = CreateOneWayClientWithRunForwarderFalse(r => r.TypeBased().Map<SomeMessage>("server"), settings);

        // Keep success rate 0 for the client's direct send, forcing it to use the outbox.
        settings.SuccessRate = 0;

        await using (var connection = new SqlConnection(ConnectionString))
        {
            await connection.OpenAsync();
            await using var transaction = connection.BeginTransaction();

            using var scope = new RebusTransactionScope();
            scope.UseOutbox(connection, transaction);
            await client.Send(new SomeMessage());
            await scope.CompleteAsync();
            await transaction.CommitAsync();
        }

        // Even after we restore direct transport success rate, the outbox forwarder is NOT running,
        // so the pending outbox message should NOT be sent.
        settings.SuccessRate = 1;

        // Wait a few seconds to verify the message is NOT received.
        var messageReceived = messageWasReceived.WaitOne(TimeSpan.FromSeconds(5));
        Assert.That(messageReceived, Is.False, "Message should NOT have been forwarded from outbox because RunForwarder is false.");
    }

    [Test]
    public async Task ForwardsMessagesWhenOutboxForwarderIsEnabled()
    {
        var settings = new FlakySenderTransportDecoratorSettings();

        using var messageWasReceived = new ManualResetEvent(initialState: false);
        using var server = CreateConsumer("server", a => a.Handle<SomeMessage>(async _ => messageWasReceived.Set()));
        using var client = CreateOneWayClientWithRunForwarderTrue(r => r.TypeBased().Map<SomeMessage>("server"), settings);

        // Keep success rate 0 for the client's direct send, forcing it to use the outbox.
        settings.SuccessRate = 0;

        await using (var connection = new SqlConnection(ConnectionString))
        {
            await connection.OpenAsync();
            await using var transaction = connection.BeginTransaction();

            using var scope = new RebusTransactionScope();
            scope.UseOutbox(connection, transaction);
            await client.Send(new SomeMessage());
            await scope.CompleteAsync();
            await transaction.CommitAsync();
        }

        // Once we restore direct transport success rate, the outbox forwarder is running,
        // so it should pick up the message and send it.
        settings.SuccessRate = 1;

        var messageReceived = messageWasReceived.WaitOne(TimeSpan.FromSeconds(10));
        Assert.That(messageReceived, Is.True, "Message should be forwarded from outbox when RunForwarder is true.");
    }

    IBus CreateConsumer(string queueName, Action<BuiltinHandlerActivator> handlers = null)
    {
        var activator = new BuiltinHandlerActivator();
        handlers?.Invoke(activator);

        Configure.With(activator)
            .Transport(t => t.UseInMemoryTransport(_network, queueName))
            .Start();

        return activator.Bus;
    }

    IBus CreateOneWayClientWithRunForwarderFalse(Action<StandardConfigurer<IRouter>> routing, FlakySenderTransportDecoratorSettings settings)
    {
        return Configure.With(new BuiltinHandlerActivator())
            .Transport(t =>
            {
                t.UseInMemoryTransportAsOneWayClient(_network);
                t.Decorate(c => new FlakySenderTransportDecorator(c.Get<ITransport>(), settings));
            })
            .Routing(r => routing?.Invoke(r))
            .Outbox(
                o => o.StoreInSqlServer(ConnectionString, "RebusOutbox"),
                new OutboxOptions { RunForwarder = false }
            )
            .Start();
    }

    IBus CreateOneWayClientWithRunForwarderTrue(Action<StandardConfigurer<IRouter>> routing, FlakySenderTransportDecoratorSettings settings)
    {
        return Configure.With(new BuiltinHandlerActivator())
            .Transport(t =>
            {
                t.UseInMemoryTransportAsOneWayClient(_network);
                t.Decorate(c => new FlakySenderTransportDecorator(c.Get<ITransport>(), settings));
            })
            .Routing(r => routing?.Invoke(r))
            .Outbox(
                o => o.StoreInSqlServer(ConnectionString, "RebusOutbox"),
                new OutboxOptions { RunForwarder = true, ForwarderIntervalSeconds = 1 }
            )
            .Start();
    }
}
