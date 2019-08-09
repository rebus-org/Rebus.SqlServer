using System;
using Rebus.Logging;
using Rebus.SqlServer;
using Rebus.SqlServer.Transport;
using Rebus.Threading;
using Rebus.Time;
using Rebus.Transport;
// ReSharper disable ArgumentsStyleNamedExpression
// ReSharper disable ArgumentsStyleLiteral
// ReSharper disable ArgumentsStyleOther

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
        public static SqlServerOptions UseSqlServerNew(this StandardConfigurer<ITransport> configurer, string connectionString, string inputQueueName)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));
            if (inputQueueName == null) throw new ArgumentNullException(nameof(inputQueueName));

            var options = new SqlServerOptions();

            configurer.Register(c =>
            {
                var connectionProvider = new DbConnectionProvider(connectionString, c.Get<IRebusLoggerFactory>());
                var rebusTime = c.Get<IRebusTime>();
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var asyncTaskFactory = c.Get<IAsyncTaskFactory>();

                return new NewSqlServerTransport(
                    connectionProvider: connectionProvider,
                    rebusTime: rebusTime,
                    asyncTaskFactory: asyncTaskFactory,
                    rebusLoggerFactory: rebusLoggerFactory,
                    inputQueueName: inputQueueName,
                    schema: options.Schema,
                    timingConfiguration: options.GetTimingConfiguration()
                );
            });

            return options;
        }

        /// <summary>
        /// Configures Rebus to use the NEW SQL Server transport, which is lease-based and works within one specific schema.
        /// </summary>
        public static SqlServerOneWayOptions UseSqlServerAsOneWayClientNew(this StandardConfigurer<ITransport> configurer, string connectionString)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));

            var options = new SqlServerOneWayOptions();

            configurer.Register(c =>
            {
                var connectionProvider = new DbConnectionProvider(connectionString, c.Get<IRebusLoggerFactory>());
                var rebusTime = c.Get<IRebusTime>();
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var asyncTaskFactory = c.Get<IAsyncTaskFactory>();

                return new NewSqlServerTransport(
                    connectionProvider: connectionProvider,
                    rebusTime: rebusTime,
                    asyncTaskFactory: asyncTaskFactory,
                    rebusLoggerFactory: rebusLoggerFactory,
                    inputQueueName: null,
                    schema: options.Schema,
                    timingConfiguration: options.GetTimingConfiguration()
                );
            });

            OneWayClientBackdoor.ConfigureOneWayClient(configurer);

            return options;
        }
    }
}