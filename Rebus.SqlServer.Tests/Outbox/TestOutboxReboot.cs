using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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
    public async Task CannotUseOutboxTwice()
    {
        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var transaction = connection.BeginTransaction();

        using var scope = new RebusTransactionScope();
        scope.UseOutbox(connection, transaction);

        Assert.Throws<InvalidOperationException>(() => scope.UseOutbox(connection, transaction));
    }

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
            .Outbox(o => o.UseSqlServerAsOneWayClient("RebusOutbox"))
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
    public static void UseSqlServerAsOneWayClient(this StandardConfigurer<IOutboxStorage> configurer, string tableName)
    {
        if (configurer == null) throw new ArgumentNullException(nameof(configurer));
        if (tableName == null) throw new ArgumentNullException(nameof(tableName));

        UseSqlServerAsOneWayClient(configurer, TableName.Parse(tableName));
    }

    public static void UseSqlServerAsOneWayClient(this StandardConfigurer<IOutboxStorage> configurer, TableName tableName)
    {
        if (configurer == null) throw new ArgumentNullException(nameof(configurer));
        if (tableName == null) throw new ArgumentNullException(nameof(tableName));

        configurer
            .OtherService<IOutboxStorage>()
            .Register(c => new SqlServerOutboxStorage(null, tableName));

        configurer
            .OtherService<ITransport>()
            .Decorate(c => new OutboxClientTransportDecorator(c.Get<ITransport>(), c.Get<IOutboxStorage>()));
    }

    public static void UseOutbox(this RebusTransactionScope rebusTransactionScope, SqlConnection connection, SqlTransaction transaction)
    {
        if (rebusTransactionScope == null) throw new ArgumentNullException(nameof(rebusTransactionScope));
        if (connection == null) throw new ArgumentNullException(nameof(connection));
        if (transaction == null) throw new ArgumentNullException(nameof(transaction));

        var context = rebusTransactionScope.TransactionContext;

        if (!context.Items.TryAdd(CurrentOutboxConnectionKey, new OutboxConnection(connection, transaction)))
        {
            throw new InvalidOperationException("Cannot add the given connection/transaction to the current Rebus transaction, because a connection/transaction has already been added!");
        }
    }

    const string CurrentOutboxConnectionKey = "current-outbox-connection";

    class OutboxClientTransportDecorator : ITransport
    {
        const string OutgoingMessagesKey = "outbox-outgoing-messages";
        readonly ITransport _transport;
        readonly IOutboxStorage _outboxStorage;

        public OutboxClientTransportDecorator(ITransport transport, IOutboxStorage outboxStorage)
        {
            _transport = transport ?? throw new ArgumentNullException(nameof(transport));
            _outboxStorage = outboxStorage ?? throw new ArgumentNullException(nameof(outboxStorage));
        }

        public void CreateQueue(string address) => _transport.CreateQueue(address);

        public Task Send(string destinationAddress, TransportMessage message, ITransactionContext context)
        {
            var connection = context.GetOrNull<OutboxConnection>(CurrentOutboxConnectionKey);

            if (connection == null)
            {
                return _transport.Send(destinationAddress, message, context);
            }

            var dbConnection = new DbConnectionWrapper(connection.Connection, connection.Transaction, managedExternally: true);

            var outgoingMessages = context.GetOrAdd(OutgoingMessagesKey, () =>
            {
                var queue = new ConcurrentQueue<AbstractRebusTransport.OutgoingMessage>();

                context.OnCommitted(async _ => await _outboxStorage.Save(queue, dbConnection));

                return queue;
            });

            outgoingMessages.Enqueue(new AbstractRebusTransport.OutgoingMessage(message, destinationAddress));

            return Task.CompletedTask;
        }

        public Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken) => _transport.Receive(context, cancellationToken);

        public string Address => _transport.Address;
    }
}
