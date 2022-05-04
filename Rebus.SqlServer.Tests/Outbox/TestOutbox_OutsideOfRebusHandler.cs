using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Config.Outbox;
using Rebus.Persistence.InMem;
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
public class TestOutbox_OutsideOfRebusHandler : FixtureBase
{
    static string ConnectionString => SqlTestHelper.ConnectionString;

    InMemNetwork _network;
    InMemorySubscriberStore _subscriberStore;

    protected override void SetUp()
    {
        base.SetUp();

        SqlTestHelper.DropTable("RebusOutbox");

        _network = new InMemNetwork();
        _subscriberStore = new InMemorySubscriberStore();
    }

    record SomeMessage;

    [Test]
    public async Task CannotUseOutboxTwice()
    {
        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var transaction = connection.BeginTransaction();

        using var scope = new RebusTransactionScope();
        scope.UseOutbox(connection, transaction);

        Assert.Throws<InvalidOperationException>(() => scope.UseOutbox(connection, transaction));
    }

    [TestCase(true, true)]
    [TestCase(false, false)]
    public async Task CanUseOutboxOutsideOfRebusHandler_Publish(bool commitTransaction, bool expectMessageToBeReceived)
    {
        var settings = new FlakySenderTransportDecoratorSettings();

        using var messageWasReceived = new ManualResetEvent(initialState: false);
        using var server = CreateConsumer("server", a => a.Handle<SomeMessage>(async _ => messageWasReceived.Set()));

        await server.Subscribe<SomeMessage>();

        using var client = CreateOneWayClient(flakySenderTransportDecoratorSettings: settings);

        // set success rate pretty low, so we're sure that it's currently not possible to use the
        // real transport - this is a job for the outbox! 
        settings.SuccessRate = 0;

        // pretending we're in a web app - we have these two bad boys at work:
        await using (var connection = new SqlConnection(ConnectionString))
        {
            await connection.OpenAsync();
            await using var transaction = connection.BeginTransaction();

            // this is how we would use the outbox for outgoing messages
            using var scope = new RebusTransactionScope();
            scope.UseOutbox(connection, transaction);
            await client.Publish(new SomeMessage());
            await scope.CompleteAsync();

            if (commitTransaction)
            {
                // this is what we were all waiting for!
                await transaction.CommitAsync();
            }
        }

        // we would not have gotten this far without the outbox - now let's pretend that the transport has recovered
        settings.SuccessRate = 1;

        // wait for server to receive the event
        Assert.That(messageWasReceived.WaitOne(TimeSpan.FromSeconds(15)), Is.EqualTo(expectMessageToBeReceived),
            $"When commitTransaction={commitTransaction} we {(expectMessageToBeReceived ? "expected the message to be sent and thus received" : "did NOT expect the message to be sent and therefore also not received")}");
    }

    [TestCase(true, true)]
    [TestCase(false, false)]
    public async Task CanUseOutboxOutsideOfRebusHandler_Send(bool commitTransaction, bool expectMessageToBeReceived)
    {
        var settings = new FlakySenderTransportDecoratorSettings();

        using var messageWasReceived = new ManualResetEvent(initialState: false);
        using var server = CreateConsumer("server", a => a.Handle<SomeMessage>(async _ => messageWasReceived.Set()));
        using var client = CreateOneWayClient(r => r.TypeBased().Map<SomeMessage>("server"), settings);

        // set success rate pretty low, so we're sure that it's currently not possible to use the
        // real transport - this is a job for the outbox! 
        settings.SuccessRate = 0;

        // pretending we're in a web app - we have these two bad boys at work:
        await using (var connection = new SqlConnection(ConnectionString))
        {
            await connection.OpenAsync();
            await using var transaction = connection.BeginTransaction();

            // this is how we would use the outbox for outgoing messages
            using var scope = new RebusTransactionScope();
            scope.UseOutbox(connection, transaction);
            await client.Send(new SomeMessage());
            await scope.CompleteAsync();

            if (commitTransaction)
            {
                // this is what we were all waiting for!
                await transaction.CommitAsync();
            }
        }

        // we would not have gotten this far without the outbox - now let's pretend that the transport has recovered
        settings.SuccessRate = 1;

        // wait for server to receive the event
        Assert.That(messageWasReceived.WaitOne(TimeSpan.FromSeconds(15)), Is.EqualTo(expectMessageToBeReceived),
            $"When commitTransaction={commitTransaction} we {(expectMessageToBeReceived ? "expected the message to be sent and thus received" : "did NOT expect the message to be sent and therefore also not received")}");
    }

    IBus CreateConsumer(string queueName, Action<BuiltinHandlerActivator> handlers = null)
    {
        var activator = new BuiltinHandlerActivator();

        handlers?.Invoke(activator);

        Configure.With(activator)
            .Transport(t => t.UseInMemoryTransport(_network, queueName))
            .Subscriptions(s => s.StoreInMemory(_subscriberStore))
            .Start();

        return activator.Bus;
    }

    IBus CreateOneWayClient(Action<StandardConfigurer<IRouter>> routing = null, FlakySenderTransportDecoratorSettings flakySenderTransportDecoratorSettings = null)
    {
        return Configure.With(new BuiltinHandlerActivator())
            .Transport(t =>
            {
                t.UseInMemoryTransportAsOneWayClient(_network);

                if (flakySenderTransportDecoratorSettings != null)
                {
                    t.Decorate(c => new FlakySenderTransportDecorator(c.Get<ITransport>(),
                        flakySenderTransportDecoratorSettings));
                }
            })
            .Routing(r => routing?.Invoke(r))
            .Subscriptions(s => s.StoreInMemory(_subscriberStore))
            .Outbox(o => o.StoreInSqlServer(ConnectionString, "RebusOutbox"))
            .Start();
    }
}