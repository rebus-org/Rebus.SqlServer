using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Rebus.Config;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.SqlServer.Outbox;
using Rebus.Threading;
using Rebus.Transport;
// ReSharper disable ArgumentsStyleLiteral

namespace Rebus.SqlServer.Tests.Outbox.Reboot;

public static class OutboxExtensions
{
    public static void StoreInSqlServer(this StandardConfigurer<IOutboxStorage> configurer, string connectionString, string tableName)
    {
        if (configurer == null) throw new ArgumentNullException(nameof(configurer));
        if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));
        if (tableName == null) throw new ArgumentNullException(nameof(tableName));

        StoreInSqlServer(configurer, connectionString, TableName.Parse(tableName));
    }

    public static void StoreInSqlServer(this StandardConfigurer<IOutboxStorage> configurer, string connectionString, TableName tableName)
    {
        if (configurer == null) throw new ArgumentNullException(nameof(configurer));
        if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));
        if (tableName == null) throw new ArgumentNullException(nameof(tableName));

        IDbConnection ConnectionProvider(ITransactionContext context)
        {
            var connection = new SqlConnection(connectionString);

            connection.Open();

            try
            {
                var transaction = connection.BeginTransaction();

                return new DbConnectionWrapper(connection, transaction, managedExternally: false);
            }
            catch
            {
                connection.Dispose();
                throw;
            }
        }

        configurer
            .OtherService<IOutboxStorage>()
            .Register(_ => new SqlServerOutboxStorage(ConnectionProvider, tableName));

        configurer
            .OtherService<ITransport>()
            .Decorate(c => new OutboxClientTransportDecorator(c.Get<ITransport>(), c.Get<IOutboxStorage>()));

        configurer
            .OtherService<OutboxForwarder>()
            .Register(c =>
            {
                var asyncTaskFactory = c.Get<IAsyncTaskFactory>();
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var outboxStorage = c.Get<IOutboxStorage>();
                var transport = c.Get<ITransport>();
                return new OutboxForwarder(asyncTaskFactory, rebusLoggerFactory, outboxStorage, transport);
            });

        configurer
            .OtherService<Options>()
            .Decorate(c =>
            {
                _ = c.Get<OutboxForwarder>();
                return c.Get<Options>();
            });
    }

    /// <summary>
    /// Enables the use of
    /// </summary>
    /// <param name="rebusTransactionScope"></param>
    /// <param name="connection"></param>
    /// <param name="transaction"></param>
    /// <exception cref="ArgumentNullException"></exception>
    /// <exception cref="InvalidOperationException"></exception>
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