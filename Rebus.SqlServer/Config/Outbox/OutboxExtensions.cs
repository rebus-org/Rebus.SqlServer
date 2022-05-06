using System;
using Microsoft.Data.SqlClient;
using Rebus.SqlServer;
using Rebus.SqlServer.Outbox;
using Rebus.Time;
using Rebus.Transport;

// ReSharper disable ArgumentsStyleLiteral

namespace Rebus.Config.Outbox;

/// <summary>
/// Configuration extensions for SQL Server-based outbox
/// </summary>
public static class OutboxExtensions
{
    internal const string CurrentOutboxConnectionKey = "current-outbox-connection";

    /// <summary>
    /// Configures SQL Server as the outbox storage
    /// </summary>
    public static OutboxOptionsBuilder StoreInSqlServer(this StandardConfigurer<IOutboxStorage> configurer, string connectionString, string tableName)
    {
        if (configurer == null) throw new ArgumentNullException(nameof(configurer));
        if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));
        if (tableName == null) throw new ArgumentNullException(nameof(tableName));

        return StoreInSqlServer(configurer, connectionString, TableName.Parse(tableName));
    }

    /// <summary>
    /// Configures SQL Server as the outbox storage
    /// </summary>
    public static OutboxOptionsBuilder StoreInSqlServer(this StandardConfigurer<IOutboxStorage> configurer, string connectionString, TableName tableName)
    {
        if (configurer == null) throw new ArgumentNullException(nameof(configurer));
        if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));
        if (tableName == null) throw new ArgumentNullException(nameof(tableName));

        var options = new OutboxOptionsBuilder();

        IDbConnection ConnectionProvider(ITransactionContext context)
        {
            // if we find a connection in the context, use that (and accept that its lifestyle is managed somewhere else):
            if (context.Items.TryGetValue(CurrentOutboxConnectionKey, out var result) && result is OutboxConnection outboxConnection)
            {
                return new DbConnectionWrapper(outboxConnection.Connection, outboxConnection.Transaction, managedExternally: true);
            }

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
            .OtherService<OutboxOptionsBuilder>()
            .Register(_ => options);

        configurer
            .OtherService<IOutboxStorage>()
            .Register(c =>
            {
                var rebusTime = c.Get<IRebusTime>();
                return new SqlServerOutboxStorage(ConnectionProvider, tableName, options, rebusTime);
            });

        configurer
            .OtherService<IOutboxConnectionProvider>()
            .Register(_ => new OutboxConnectionProvider(connectionString));

        return options;
    }

    /// <summary>
    /// Enables the use of outbox on the <see cref="RebusTransactionScope"/>. Will enlist all outgoing message operations in the
    /// <paramref name="connection"/>/<paramref name="transaction"/> passed to the method.
    /// </summary>
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
}