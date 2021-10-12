using System;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Rebus.Logging;
using Rebus.Pipeline;
using Rebus.SqlServer;
using Rebus.SqlServer.Outbox;
using Rebus.Threading;
using Rebus.Time;
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

                o.Decorate<ITransport>(c =>
                {
                    var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                    var transport = c.Get<ITransport>();
                    var asyncTaskFactory = c.Get<IAsyncTaskFactory>();
                    var outboxStorage = c.Get<IOutboxStorage>();
                    return new OutboxTransportDecorator(
                        rebusLoggerFactory,
                        transport,
                        outboxStorage,
                        asyncTaskFactory
                    );
                });

                o.Decorate<IPipeline>(c => new PipelineStepConcatenator(c.Get<IPipeline>()));
            });

            return configurer;
        }

        /// <summary>
        /// Configures the outbox to use SQL Server to store outgoing messages. This overload will NOT enlist outbox actions in user transactions.
        /// </summary>
        public static void UseSqlServer(this StandardConfigurer<IOutboxStorage> configurer, string connectionString, string tableName)
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

            configurer.Register(c => new SqlServerOutboxStorage(ConnectionProvider, TableName.Parse(tableName)));
        }

        /// <summary>
        /// Configures the outbox to use SQL Server to store outgoing messages. The <paramref name="connectionProvider"/> gets to
        /// return the <see cref="IDbConnection"/> the outbox should use, thus providing the ability to return a reference to the same
        /// connection/transaction that the user's code is using.
        /// </summary>
        public static void UseSqlServer(this StandardConfigurer<IOutboxStorage> configurer, Func<ITransactionContext, IDbConnection> connectionProvider, string tableName)
        {
            configurer.Register(c => new SqlServerOutboxStorage(connectionProvider, TableName.Parse(tableName)));
        }
    }
}
