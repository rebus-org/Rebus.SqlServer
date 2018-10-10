using System;
using System.Threading.Tasks;
using Rebus.DataBus;
using Rebus.Logging;
using Rebus.SqlServer;
using Rebus.SqlServer.DataBus;

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
        public static void StoreInSqlServer(this StandardConfigurer<IDataBusStorage> configurer, string connectionStringOrConnectionStringName, string tableName, bool automaticallyCreateTables = true, int commandTimeout = 240
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
                var loggerFactory = c.Get<IRebusLoggerFactory>();
                var connectionProvider = new DbConnectionProvider(connectionStringOrConnectionStringName, loggerFactory
#if HAS_AMBIENT_TRANSACTIONS
                    , enlistInAmbientTransaction
#endif
                );
                return new SqlServerDataBusStorage(connectionProvider, tableName, automaticallyCreateTables, loggerFactory, commandTimeout);
            });
        }

        /// <summary>
        /// Configures the data bus to store data in a central SQL Server 
        /// </summary>
        public static void StoreInSqlServer(this StandardConfigurer<IDataBusStorage> configurer, Func<Task<IDbConnection>> connectionFactory, string tableName, bool automaticallyCreateTables = true, int commandTimeout = 240)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (connectionFactory == null) throw new ArgumentNullException(nameof(connectionFactory));
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));

            configurer.Register(c =>
            {
                var loggerFactory = c.Get<IRebusLoggerFactory>();
                var connectionProvider = new DbConnectionFactoryProvider(connectionFactory, loggerFactory);
                return new SqlServerDataBusStorage(connectionProvider, tableName, automaticallyCreateTables, loggerFactory, commandTimeout);
            });
        }
    }
}