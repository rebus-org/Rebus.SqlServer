using System;
using Microsoft.Data.SqlClient;
using Rebus.SqlServer;
using Rebus.SqlServer.Outbox;
using Rebus.Transport;

namespace Rebus.Config.Outbox
{
    /// <summary>
    /// Configuration extensions for the experimental outbox support
    /// </summary>
    public static class SqlServerOutboxConfigurationExtensions
    {
        /// <summary>
        /// Configures Rebus to use an outbox.
        /// This will store a (message ID, source queue) tuple for all processed messages, and under this tuple any messages sent/published will
        /// also be stored, thus enabling truly idempotent message processing.
        /// </summary>
        public static RebusConfigurer Outbox(this RebusConfigurer configurer, Action<StandardConfigurer<IOutboxStorage>> configure)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (configure == null) throw new ArgumentNullException(nameof(configure));

            configurer.Options(o =>
            {
                configure(StandardConfigurer<IOutboxStorage>.GetConfigurerFrom(o));

                // if no outbox storage was registered, no further calls must have been made... that's ok, so we just bail out here
                if (!o.Has<IOutboxStorage>()) return;

                o.Decorate<ITransport>(c => new OutboxTransportDecorator(c.Get<ITransport>(), c.Get<IOutboxStorage>()));
            });

            return configurer;
        }

        /// <summary>
        /// Configures the outbox to use SQL Server to store outgoing messages
        /// </summary>
        public static void UseSqlServer(this StandardConfigurer<IOutboxStorage> configurer, string connectionString, string tableName)
        {
            configurer.Register(c =>
            {
                IDbConnection ConnectionProvider(ITransactionContext context)
                {
                    var sqlConnection = new SqlConnection(connectionString);
                    sqlConnection.Open();
                    try
                    {
                        var transaction = sqlConnection.BeginTransaction();
                        return new DbConnectionWrapper(sqlConnection, transaction, managedExternally: false);
                    }
                    catch
                    {
                        sqlConnection.Dispose();
                        throw;
                    }
                }

                return new SqlServerOutboxStorage(ConnectionProvider, TableName.Parse(tableName));
            });
        }
    }
}
