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
        public static void StoreInSqlServer(this StandardConfigurer<IDataBusStorage> configurer, string connectionString, string tableName, bool automaticallyCreateTables = true, int commandTimeout = 240, bool enlistInAmbientTransaction = false)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));

            configurer.Register(c =>
            {
                var loggerFactory = c.Get<IRebusLoggerFactory>();
                var connectionProvider = new DbConnectionProvider(connectionString, loggerFactory, enlistInAmbientTransaction);
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