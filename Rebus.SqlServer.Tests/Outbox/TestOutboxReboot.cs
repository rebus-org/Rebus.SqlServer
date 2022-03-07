using System;
using System.Collections.Concurrent;
using System.Runtime.InteropServices.ComTypes;
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
using Rebus.SqlServer.Outbox;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Utilities;
using Rebus.Transport;
using Rebus.Transport.InMem;
// ReSharper disable ArgumentsStyleLiteral
// ReSharper disable AccessToDisposedClosure
#pragma warning disable CS1998

namespace Rebus.SqlServer.Tests.Outbox;

[TestFixture]
public class TestOutboxReboot : FixtureBase
{
    static string ConnectionString => SqlTestHelper.ConnectionString;

    InMemNetwork _network;

    protected override void SetUp()
    {
        base.SetUp();

        _network = new InMemNetwork();
    }

    record SomeMessage;

    [Test]
    [Description("One scenario where the SQL outbox works: Outside of Rebus handlers, e.g. in a web app, it's great to be able to send even though the bus is offline")]
    public async Task CanUseOutboxOutsideOfRebusHandler()
    {
        var settings = new FlakySenderTransportDecoratorSettings();

        using var counter = new SharedCounter(initialValue: 1);
        using var server = CreateServer("server", a => a.Handle<SomeMessage>(async _ => counter.Decrement()));
        using var client = CreateOneWayClient(r => r.TypeBased().Map<SomeMessage>("server"), settings);

        // set success rate pretty low, so we're sure that it's currently not possible to use the
        // real transport - this is a job for the outbox! 
        settings.SuccessRate = 0;

        // pretending we're in a web app - we have these two bad boys at work:
        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var transaction = connection.BeginTransaction();

        // this is how we would use the outbox for outgoing messages
        using var scope = new RebusTransactionScope();
        scope.UseOutbox(connection, transaction);
        await client.Send(new SomeMessage());
        await scope.CompleteAsync();

        // we would not have gotten this far without the outbox - now let's pretend that the transport has recovered
        settings.SuccessRate = 1;

        // wait for server to receive the event
        counter.WaitForResetEvent();
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
            .Outbox(o => o.UseSqlServerAsOneWayClient())
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

public static class OutboxExtensions
{
    public static void UseSqlServerAsOneWayClient(this StandardConfigurer<IOutboxStorage> configurer)
    {
        if (configurer == null) throw new ArgumentNullException(nameof(configurer));

        configurer
            .OtherService<ITransport>()
            .Decorate(c => new OutboxClientTransportDecorator(c.Get<ITransport>()));
    }

    public static void UseOutbox(this RebusTransactionScope rebusTransactionScope, SqlConnection connection, SqlTransaction transaction)
    {
        if (rebusTransactionScope == null) throw new ArgumentNullException(nameof(rebusTransactionScope));
        if (connection == null) throw new ArgumentNullException(nameof(connection));
        if (transaction == null) throw new ArgumentNullException(nameof(transaction));


    }

    class OutboxClientTransportDecorator : ITransport
    {
        const string OutgoingMessagesKey = "outbox-outgoing-messages";
        readonly ITransport _transport;

        public OutboxClientTransportDecorator(ITransport transport) => _transport = transport ?? throw new ArgumentNullException(nameof(transport));

        public void CreateQueue(string address) => _transport.CreateQueue(address);

        public Task Send(string destinationAddress, TransportMessage message, ITransactionContext context)
        {
            return _transport.Send(destinationAddress, message, context);

            var outgoingMessages = context.GetOrAdd(OutgoingMessagesKey, () =>
            {
                var queue = new ConcurrentQueue<AbstractRebusTransport.OutgoingMessage>();

                return queue;
            });

            outgoingMessages.Enqueue(new AbstractRebusTransport.OutgoingMessage(message, destinationAddress));
        }

        public Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken) => _transport.Receive(context, cancellationToken);

        public string Address => _transport.Address;
    }
}
