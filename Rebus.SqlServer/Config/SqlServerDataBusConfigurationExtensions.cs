using System;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Rebus.DataBus;
using Rebus.Logging;
using Rebus.SqlServer;
using Rebus.SqlServer.DataBus;
using Rebus.Time;

namespace Rebus.Config
{
    /// <summary>
    /// Configuration extensions for SQL Server data bus
    /// </summary>
    public static class SqlServerDataBusConfigurationExtensions
    {
        /// <summary>
        /// Configures the data bus to store data in a central SQL Server 
        /// </summary>
        public static void StoreInSqlServer(this StandardConfigurer<IDataBusStorage> configurer, string connectionString, string tableName, bool automaticallyCreateTables = true, int commandTimeout = 240, bool enlistInAmbientTransaction = false)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));

            configurer.OtherService<SqlServerDataBusStorage>().Register(c =>
            {
                var rebusTime = c.Get<IRebusTime>();
                var loggerFactory = c.Get<IRebusLoggerFactory>();
                var connectionProvider = new DbConnectionProvider(connectionString, loggerFactory, enlistInAmbientTransaction);
                return new SqlServerDataBusStorage(connectionProvider, tableName, automaticallyCreateTables, loggerFactory, rebusTime, commandTimeout);
            });

            configurer.Register(c => c.Get<SqlServerDataBusStorage>());

            configurer.OtherService<IDataBusStorageManagement>().Register(c => c.Get<SqlServerDataBusStorage>());
        }

        /// <summary>
        /// Configures the data bus to store data in a central SQL Server 
        /// </summary>
        public static void StoreInSqlServer(this StandardConfigurer<IDataBusStorage> configurer, Func<Task<IDbConnection>> connectionFactory, string tableName, bool automaticallyCreateTables = true, int commandTimeout = 240)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (connectionFactory == null) throw new ArgumentNullException(nameof(connectionFactory));
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));

            configurer.OtherService<SqlServerDataBusStorage>().Register(c =>
            {
                var rebusTime = c.Get<IRebusTime>();
                var loggerFactory = c.Get<IRebusLoggerFactory>();
                var connectionProvider = new DbConnectionFactoryProvider(connectionFactory);
                return new SqlServerDataBusStorage(connectionProvider, tableName, automaticallyCreateTables, loggerFactory, rebusTime, commandTimeout);
            });

            configurer.Register(c => c.Get<SqlServerDataBusStorage>());

            configurer.OtherService<IDataBusStorageManagement>().Register(c => c.Get<SqlServerDataBusStorage>());
        }

        /// <summary>
        /// Configures the data bus to store data in a central SQL Server 
        /// </summary>
        public static void StoreInSqlServer(this StandardConfigurer<IDataBusStorage> configurer, SqlServerDataBusOptions options, string tableName)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));

            configurer.OtherService<SqlServerDataBusStorage>().Register(c =>
            {
                var connectionProvider = options.ConnectionProviderFactory(c);
                var rebusTime = c.Get<IRebusTime>();
                var loggerFactory = c.Get<IRebusLoggerFactory>();
                var automaticallyCreateTables = options.EnsureTablesAreCreated;
                var commandTimeoutSeconds = (int)options.CommandTimeout.TotalSeconds;
                
                return new SqlServerDataBusStorage(
                    connectionProvider: connectionProvider,
                    tableName: tableName,
                    ensureTableIsCreated: automaticallyCreateTables,
                    rebusLoggerFactory: loggerFactory,
                    rebusTime: rebusTime,
                    commandTimeout: commandTimeoutSeconds
                );
            });

            configurer.Register(c => c.Get<SqlServerDataBusStorage>());

            configurer.OtherService<IDataBusStorageManagement>().Register(c => c.Get<SqlServerDataBusStorage>());

        }
    }
}
