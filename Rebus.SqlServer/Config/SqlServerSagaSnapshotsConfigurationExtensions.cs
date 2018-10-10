using System;
using System.Threading.Tasks;
using Rebus.Auditing.Sagas;
using Rebus.Logging;
using Rebus.SqlServer;
using Rebus.SqlServer.Sagas;

namespace Rebus.Config
{
    /// <summary>
    /// Configuration extensions for saga snapshots
    /// </summary>
    public static class SqlServerSagaSnapshotsConfigurationExtensions
    {
        /// <summary>
        /// Configures Rebus to store saga snapshots in SQL Server
        /// </summary>
        public static void StoreInSqlServer(this StandardConfigurer<ISagaSnapshotStorage> configurer,
            string connectionStringOrConnectionStringName, string tableName, bool automaticallyCreateTables = true
#if HAS_AMBIENT_TRANSACTIONS
            , bool enlistInAmbientTransaction = false
#endif
            )
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (connectionStringOrConnectionStringName == null) throw new ArgumentNullException(nameof(connectionStringOrConnectionStringName));
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));

            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var connectionProvider = new DbConnectionProvider(connectionStringOrConnectionStringName, rebusLoggerFactory
#if HAS_AMBIENT_TRANSACTIONS
                    , enlistInAmbientTransaction
#endif
                    );
                var snapshotStorage = new SqlServerSagaSnapshotStorage(connectionProvider, tableName, rebusLoggerFactory);

                if (automaticallyCreateTables)
                {
                    snapshotStorage.EnsureTableIsCreated();
                }

                return snapshotStorage;
            });
        }

        /// <summary>
        /// Configures Rebus to store saga snapshots in SQL Server
        /// </summary>
        public static void StoreInSqlServer(this StandardConfigurer<ISagaSnapshotStorage> configurer,
            Func<Task<IDbConnection>> connectionFactory, string tableName, bool automaticallyCreateTables = true)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (connectionFactory == null) throw new ArgumentNullException(nameof(connectionFactory));
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));

            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var connectionProvider = new DbConnectionFactoryProvider(connectionFactory, rebusLoggerFactory);
                var snapshotStorage = new SqlServerSagaSnapshotStorage(connectionProvider, tableName, rebusLoggerFactory);

                if (automaticallyCreateTables)
                {
                    snapshotStorage.EnsureTableIsCreated();
                }

                return snapshotStorage;
            });
        }
    }
}