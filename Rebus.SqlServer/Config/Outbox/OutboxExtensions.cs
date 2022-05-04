using System;
using Microsoft.Data.SqlClient;
using Rebus.SqlServer;
using Rebus.SqlServer.Outbox;
using Rebus.Transport;

// ReSharper disable ArgumentsStyleLiteral

namespace Rebus.Config.Outbox;

public static class OutboxExtensions
{
    internal const string CurrentOutboxConnectionKey = "current-outbox-connection";

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
}