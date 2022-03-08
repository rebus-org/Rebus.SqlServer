using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Config.Outbox;
using Rebus.Messages;
using Rebus.Routing;
using Rebus.Routing.TypeBased;
using Rebus.Tests.Contracts;
using Rebus.Transport;
using Rebus.Transport.InMem;

// ReSharper disable ArgumentsStyleLiteral
// ReSharper disable AccessToDisposedClosure
#pragma warning disable CS1998

namespace Rebus.SqlServer.Tests.Outbox.Reboot;

[TestFixture]
public class TestOutboxReboot : FixtureBase
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
    public async Task CanUseOutboxOutsideOfRebusHandler_Commit(bool commitTransaction, bool expectMessageToBeReceived)
    {
        var settings = new FlakySenderTransportDecoratorSettings();

        using var messageWasReceived = new ManualResetEvent(initialState: false);
        using var server = CreateServer("server", a => a.Handle<SomeMessage>(async _ => messageWasReceived.Set()));
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

    IDisposable CreateServer(string queueName, Action<BuiltinHandlerActivator> handlers = null)
    {
        var activator = new BuiltinHandlerActivator();

        handlers?.Invoke(activator);

        Configure.With(activator)
            .Transport(t => t.UseInMemoryTransport(_network, queueName))
            .Start();

        return activator;
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
            .Outbox(o => o.UseSqlServer(ConnectionString, "RebusOutbox"))
            .Start();
    }

    class FlakySenderTransportDecoratorSettings
    {
        public double SuccessRate { get; set; } = 1;
    }

    class FlakySenderTransportDecorator : ITransport
    {
        readonly ITransport _transport;
        readonly FlakySenderTransportDecoratorSettings _flakySenderTransportDecoratorSettings;

        public FlakySenderTransportDecorator(ITransport transport,
            FlakySenderTransportDecoratorSettings flakySenderTransportDecoratorSettings)
        {
            _transport = transport;
            _flakySenderTransportDecoratorSettings = flakySenderTransportDecoratorSettings;
        }

        public void CreateQueue(string address) => _transport.CreateQueue(address);

        public Task Send(string destinationAddress, TransportMessage message, ITransactionContext context)
        {
            if (Random.Shared.NextDouble() > _flakySenderTransportDecoratorSettings.SuccessRate)
            {
                throw new RandomUnluckyException();
            }

            return _transport.Send(destinationAddress, message, context);
        }

        public Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
            return _transport.Receive(context, cancellationToken);
        }

        public string Address { get; }
    }

    class RandomUnluckyException : ApplicationException
    {
        public RandomUnluckyException() : base("You were unfortunate")
        {
        }
    }
}