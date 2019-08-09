using System;
using Rebus.Logging;
using Rebus.SqlServer;
using Rebus.SqlServer.Transport;
using Rebus.Time;
using Rebus.Transport;

namespace Rebus.Config
{
    /// <summary>
    /// Configuration extensions for the SQL transport
    /// </summary>
    public static class NewSqlServerTransportConfigurationExtensions
    {
        /// <summary>
        /// Configures Rebus to use the NEW SQL Server transport, which is lease-based and works within one specific schema.
        /// </summary>
        public static SqlServerOptions UseSqlServer(this StandardConfigurer<ITransport> configurer, string connectionString, string inputQueueName)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));
            if (inputQueueName == null) throw new ArgumentNullException(nameof(inputQueueName));

            var options = new SqlServerOptions();

            configurer.Register(c =>
            {
                var connectionProvider = new DbConnectionProvider(connectionString, c.Get<IRebusLoggerFactory>());
                var rebusTime = c.Get<IRebusTime>();
                return new NewSqlServerTransport(connectionProvider, rebusTime, inputQueueName, options.Schema);
            });

            return options;
        }

        /// <summary>
        /// Configures Rebus to use the NEW SQL Server transport, which is lease-based and works within one specific schema.
        /// </summary>
        public static SqlServerOneWayOptions UseSqlServerAsOneWayClient(this StandardConfigurer<ITransport> configurer, string connectionString)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));

            var options = new SqlServerOneWayOptions();

            configurer.Register(c =>
            {
                var connectionProvider = new DbConnectionProvider(connectionString, c.Get<IRebusLoggerFactory>());
                var rebusTime = c.Get<IRebusTime>();
                return new NewSqlServerTransport(connectionProvider, rebusTime, null, options.Schema);
            });

            return options;
        }
    }
}