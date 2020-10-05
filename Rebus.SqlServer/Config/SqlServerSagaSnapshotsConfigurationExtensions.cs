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
            string connectionString, string tableName, bool automaticallyCreateTables = true, bool enlistInAmbientTransaction = false)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));

            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var connectionProvider = new DbConnectionProvider(connectionString, rebusLoggerFactory, enlistInAmbientTransaction);
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
                var connectionProvider = new DbConnectionFactoryProvider(connectionFactory);
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
        public static void StoreInSqlServer(this StandardConfigurer<ISagaSnapshotStorage> configurer, SqlServerSagaSnapshotStorageOptions options, string tableName)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (options == null) throw new ArgumentNullException(nameof(options));
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));

            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var connectionProvider = options.ConnectionProviderFactory(c);
                var snapshotStorage = new SqlServerSagaSnapshotStorage(connectionProvider, tableName, rebusLoggerFactory);

                if (options.EnsureTablesAreCreated)
                {
                    snapshotStorage.EnsureTableIsCreated();
                }

                return snapshotStorage;
            });
        }
    }
}
