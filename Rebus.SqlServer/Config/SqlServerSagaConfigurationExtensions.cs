using System;
using System.Threading.Tasks;
using Rebus.Logging;
using Rebus.Sagas;
using Rebus.SqlServer;
using Rebus.SqlServer.Sagas;

namespace Rebus.Config
{
    /// <summary>
    /// Configuration extensions for sagas
    /// </summary>
    public static class SqlServerSagaConfigurationExtensions
    {
        /// <summary>
        /// Configures Rebus to use SQL Server to store sagas, using the tables specified to store data and indexed properties respectively.
        /// </summary>
        public static void StoreInSqlServer(this StandardConfigurer<ISagaStorage> configurer,
            string connectionString, string dataTableName, string indexTableName,
            bool automaticallyCreateTables = true, bool enlistInAmbientTransaction = false
    )
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));
            if (dataTableName == null) throw new ArgumentNullException(nameof(dataTableName));
            if (indexTableName == null) throw new ArgumentNullException(nameof(indexTableName));

            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var connectionProvider = new DbConnectionProvider(connectionString, rebusLoggerFactory, enlistInAmbientTransaction);
                var sagaStorage = new SqlServerSagaStorage(connectionProvider, dataTableName, indexTableName, rebusLoggerFactory);

                if (automaticallyCreateTables)
                {
                    sagaStorage.EnsureTablesAreCreated();
                }

                return sagaStorage;
            });
        }

        /// <summary>
        /// Configures Rebus to use SQL Server to store sagas, using the tables specified to store data and indexed properties respectively.
        /// </summary>
        public static void StoreInSqlServer(this StandardConfigurer<ISagaStorage> configurer,
            Func<Task<IDbConnection>> connectionFactory, string dataTableName, string indexTableName,
            bool automaticallyCreateTables = true)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (connectionFactory == null) throw new ArgumentNullException(nameof(connectionFactory));
            if (dataTableName == null) throw new ArgumentNullException(nameof(dataTableName));
            if (indexTableName == null) throw new ArgumentNullException(nameof(indexTableName));

            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var connectionProvider = new DbConnectionFactoryProvider(connectionFactory, rebusLoggerFactory);
                var sagaStorage = new SqlServerSagaStorage(connectionProvider, dataTableName, indexTableName, rebusLoggerFactory);

                if (automaticallyCreateTables)
                {
                    sagaStorage.EnsureTablesAreCreated();
                }

                return sagaStorage;
            });
        }
    }
}