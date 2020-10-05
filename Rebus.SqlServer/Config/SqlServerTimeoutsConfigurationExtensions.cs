using System;
using System.Threading.Tasks;
using Rebus.Logging;
using Rebus.SqlServer;
using Rebus.SqlServer.Timeouts;
using Rebus.Time;
using Rebus.Timeouts;

namespace Rebus.Config
{
    /// <summary>
    /// Configuration extensions for configuring SQL persistence for sagas, subscriptions, and timeouts.
    /// </summary>
    public static class SqlServerTimeoutsConfigurationExtensions
    {
        /// <summary>
        /// Configures Rebus to use SQL Server to store timeouts.
        /// </summary>
        public static void StoreInSqlServer(this StandardConfigurer<ITimeoutManager> configurer,
            string connectionString, string tableName, bool automaticallyCreateTables = true, bool enlistInAmbientTransaction = false)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));

            configurer.Register(c =>
            {
                var rebusTime = c.Get<IRebusTime>();
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var connectionProvider = new DbConnectionProvider(connectionString, rebusLoggerFactory, enlistInAmbientTransaction);
                var subscriptionStorage = new SqlServerTimeoutManager(connectionProvider, tableName, rebusLoggerFactory, rebusTime);

                if (automaticallyCreateTables)
                {
                    subscriptionStorage.EnsureTableIsCreated();
                }

                return subscriptionStorage;
            });
        }

        /// <summary>
        /// Configures Rebus to use SQL Server to store timeouts.
        /// </summary>
        public static void StoreInSqlServer(this StandardConfigurer<ITimeoutManager> configurer,
            Func<Task<IDbConnection>> connectionFactory, string tableName, bool automaticallyCreateTables = true)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (connectionFactory == null) throw new ArgumentNullException(nameof(connectionFactory));
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));

            configurer.Register(c =>
            {
                var rebusTime = c.Get<IRebusTime>();
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var connectionProvider = new DbConnectionFactoryProvider(connectionFactory);
                var subscriptionStorage = new SqlServerTimeoutManager(connectionProvider, tableName, rebusLoggerFactory, rebusTime);

                if (automaticallyCreateTables)
                {
                    subscriptionStorage.EnsureTableIsCreated();
                }

                return subscriptionStorage;
            });
        }

        /// <summary>
        /// Configures Rebus to use SQL Server to store timeouts.
        /// </summary>
        public static void StoreInSqlServer(this StandardConfigurer<ITimeoutManager> configurer, SqlServerTimeoutManagerOptions options, string tableName)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (options == null) throw new ArgumentNullException(nameof(options));
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));

            configurer.Register(c =>
            {
                var rebusTime = c.Get<IRebusTime>();
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var connectionProvider = options.ConnectionProviderFactory(c);
                var subscriptionStorage = new SqlServerTimeoutManager(connectionProvider, tableName, rebusLoggerFactory, rebusTime);

                if (options.EnsureTablesAreCreated)
                {
                    subscriptionStorage.EnsureTableIsCreated();
                }

                return subscriptionStorage;
            });
        }
    }
}
