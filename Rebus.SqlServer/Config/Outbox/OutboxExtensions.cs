﻿using System;
using System.Transactions;
using Microsoft.Data.SqlClient;
using Rebus.SqlServer;
using Rebus.SqlServer.Outbox;
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
    public static void StoreInSqlServer(this StandardConfigurer<IOutboxStorage> configurer, string connectionString, string tableName)
    {
        if (configurer == null) throw new ArgumentNullException(nameof(configurer));
        if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));
        if (tableName == null) throw new ArgumentNullException(nameof(tableName));

        StoreInSqlServer(configurer, connectionString, TableName.Parse(tableName));
    }

    /// <summary>
    /// Configures SQL Server as the outbox storage
    /// </summary>
    public static void StoreInSqlServer(this StandardConfigurer<IOutboxStorage> configurer, string connectionString, TableName tableName)
    {
        if (configurer == null) throw new ArgumentNullException(nameof(configurer));
        if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));
        if (tableName == null) throw new ArgumentNullException(nameof(tableName));

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
            .OtherService<IOutboxStorage>()
            .Register(_ => new SqlServerOutboxStorage(ConnectionProvider, tableName));

        configurer.OtherService<IOutboxConnectionProvider>()
            .Register(_ => new OutboxConnectionProvider(connectionString));
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

    /// <summary>
    /// Enables the use of outbox on the <see cref="RebusTransactionScope"/> by enlisting in the current ambient transaction.
    /// This requires that a <see cref="TransactionScope"/> has been started beforehand.
    /// </summary>
    public static void UseOutboxWithAmbientTransaction(this RebusTransactionScope rebusTransactionScope)
    {
        if (rebusTransactionScope == null) throw new ArgumentNullException(nameof(rebusTransactionScope));

        if (Transaction.Current == null)
        {
            throw new InvalidOperationException(
                @"This method requires that an ambient transaction has been started before being called.

You do this by creating a TransactionScope - and PLEASE PLEASE PLEASE REMEMBER to start it with new TransactionScope(TransactionScopeAsyncFlowOption.Enabled) !!!!");
        }

        var context = rebusTransactionScope.TransactionContext;

        if (!context.Items.TryAdd(CurrentOutboxConnectionKey, new OutboxConnection(connection, transaction)))
        {
            throw new InvalidOperationException("Cannot add the given connection/transaction to the current Rebus transaction, because a connection/transaction has already been added!");
        }
    }
}