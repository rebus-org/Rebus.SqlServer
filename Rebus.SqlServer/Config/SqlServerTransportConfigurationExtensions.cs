using System;
using System.Threading.Tasks;
using Rebus.Injection;
using Rebus.Logging;
using Rebus.Pipeline;
using Rebus.Pipeline.Receive;
using Rebus.SqlServer;
using Rebus.SqlServer.Transport;
using Rebus.Threading;
using Rebus.Timeouts;
using Rebus.Transport;

namespace Rebus.Config
{
    /// <summary>
    /// Configuration extensions for the SQL transport
    /// </summary>
    public static class SqlServerTransportConfigurationExtensions
    {
        /// <summary>
        /// Configures Rebus to use SQL Server as its transport (in "one-way client mode", i.e. as a send-only endpoint). Unlike the <c>UseSqlServer</c> calls the leased version of the SQL 
        /// Server transport does not hold a transaction open for the entire duration of the message handling. Instead it marks a
        /// message as being "leased" for a period of time. If the lease has expired then a worker is permitted to acquire the that
        /// message again and try reprocessing
        /// </summary>
        /// <param name="configurer">Static to extend</param>
        /// <param name="connectionStringOrConnectionStringName">Connection string or the named connection</param>
        /// <param name="tableName">Name of the table to store messages in</param>
        /// <param name="leaseInterval">If <c>null</c> will default to <seealso cref="SqlServerLeaseTransport.DefaultLeaseTime"/>. Specifies how long a worker will request to keep a message. Higher values require less database communication but increase latency of a message being processed if a worker dies</param>
        /// <param name="leaseTolerance">If <c>null</c> defaults to <seealso cref="SqlServerLeaseTransport.DefaultLeaseTolerance"/>. Workers will wait for this amount of time to elapse, beyond the lease time, before they pick up an already leased message.</param>
        /// <param name="automaticallyRenewLeases">If <c>true</c> then workers will automatically renew the lease they have acquired whilst they're still processing the message. This will occur in accordance with <paramref name="leaseAutoRenewInterval"/></param>
        /// <param name="leaseAutoRenewInterval">If <c>null</c> defaults to <seealso cref="SqlServerLeaseTransport.DefaultLeaseAutomaticRenewal"/>. Specifies how frequently a lease will be renewed whilst the worker is processing a message. Lower values decrease the chance of other workers processing the same message but increase DB communication. A value 50% of <paramref name="leaseInterval"/> should be appropriate</param>
        /// <param name="leasedByFactory">If non-<c>null</c> a factory which returns a string identifying this worker when it leases a message. If <c>null></c> the current machine name is used</param>
        public static void UseSqlServerInLeaseModeAsOneWayClient(this StandardConfigurer<ITransport> configurer, string connectionStringOrConnectionStringName, string tableName, TimeSpan? leaseInterval = null, TimeSpan? leaseTolerance = null, bool automaticallyRenewLeases = false, TimeSpan? leaseAutoRenewInterval = null, Func<string> leasedByFactory = null)
        {
            ConfigureInLeaseMode(configurer, loggerFactory => new DbConnectionProvider(connectionStringOrConnectionStringName, loggerFactory), tableName, null, leaseInterval, leaseTolerance, automaticallyRenewLeases, leaseAutoRenewInterval);
        }

        /// <summary>
        /// Configures Rebus to use SQL Server as its transport (in "one-way client mode", i.e. as a send-only endpoint). The table specified by <paramref name="tableName"/> will be used to
        /// store messages. The message table will automatically be created if it does not exist.
        /// </summary>
        /// <param name="configurer">Static to extend</param>
        /// <param name="connectionFactory">Factory to provide a new connection</param>
        /// <param name="tableName">Name of the table to store messages in</param>
        /// <param name="leaseInterval">If <c>null</c> will default to <seealso cref="SqlServerLeaseTransport.DefaultLeaseTime"/>. Specifies how long a worker will request to keep a message. Higher values require less database communication but increase latency of a message being processed if a worker dies</param>
        /// <param name="leaseTolerance">If <c>null</c> defaults to <seealso cref="SqlServerLeaseTransport.DefaultLeaseTolerance"/>. Workers will wait for this amount of time to elapse, beyond the lease time, before they pick up an already leased message.</param>
        /// <param name="automaticallyRenewLeases">If <c>true</c> then workers will automatically renew the lease they have acquired whilst they're still processing the message. This will occur in accordance with <paramref name="leaseAutoRenewInterval"/></param>
        /// <param name="leaseAutoRenewInterval">If <c>null</c> defaults to <seealso cref="SqlServerLeaseTransport.DefaultLeaseAutomaticRenewal"/>. Specifies how frequently a lease will be renewed whilst the worker is processing a message. Lower values decrease the chance of other workers processing the same message but increase DB communication. A value 50% of <paramref name="leaseInterval"/> should be appropriate</param>
        /// <param name="leasedByFactory">If non-<c>null</c> a factory which returns a string identifying this worker when it leases a message. If <c>null></c> the current machine name is used</param>
        public static void UseSqlServerInLeaseModeAsOneWayClient(this StandardConfigurer<ITransport> configurer, Func<Task<IDbConnection>> connectionFactory, string tableName, TimeSpan? leaseInterval = null, TimeSpan? leaseTolerance = null, bool automaticallyRenewLeases = false, TimeSpan? leaseAutoRenewInterval = null, Func<string> leasedByFactory = null)
        {
            ConfigureInLeaseMode(configurer, loggerFactory => new DbConnectionFactoryProvider(connectionFactory, loggerFactory), tableName, null, leaseInterval, leaseTolerance, automaticallyRenewLeases, leaseAutoRenewInterval);
        }

        /// <summary>
        /// Configures Rebus to use SQL Server as its transport. Unlike the <c>UseSqlServer</c> calls the leased version of the SQL 
        /// Server transport does not hold a transaction open for the entire duration of the message handling. Instead it marks a
        /// message as being "leased" for a period of time. If the lease has expired then a worker is permitted to acquire the that
        /// message again and try reprocessing
        /// </summary>
        /// <param name="configurer">Static to extend</param>
        /// <param name="connectionStringOrConnectionStringName">Connection string or the named connection</param>
        /// <param name="tableName">Name of the table to store messages in</param>
        /// <param name="inputQueueName">Name of the queue being services</param>
        /// <param name="leaseInterval">If <c>null</c> will default to <seealso cref="SqlServerLeaseTransport.DefaultLeaseTime"/>. Specifies how long a worker will request to keep a message. Higher values require less database communication but increase latency of a message being processed if a worker dies</param>
        /// <param name="leaseTolerance">If <c>null</c> defaults to <seealso cref="SqlServerLeaseTransport.DefaultLeaseTolerance"/>. Workers will wait for this amount of time to elapse, beyond the lease time, before they pick up an already leased message.</param>
        /// <param name="automaticallyRenewLeases">If <c>true</c> then workers will automatically renew the lease they have acquired whilst they're still processing the message. This will occur in accordance with <paramref name="leaseAutoRenewInterval"/></param>
        /// <param name="leaseAutoRenewInterval">If <c>null</c> defaults to <seealso cref="SqlServerLeaseTransport.DefaultLeaseAutomaticRenewal"/>. Specifies how frequently a lease will be renewed whilst the worker is processing a message. Lower values decrease the chance of other workers processing the same message but increase DB communication. A value 50% of <paramref name="leaseInterval"/> should be appropriate</param>
        /// <param name="leasedByFactory">If non-<c>null</c> a factory which returns a string identifying this worker when it leases a message. If <c>null></c> the current machine name is used</param>
        public static void UseSqlServerInLeaseMode(this StandardConfigurer<ITransport> configurer, string connectionStringOrConnectionStringName, string tableName, string inputQueueName, TimeSpan? leaseInterval = null, TimeSpan? leaseTolerance = null, bool automaticallyRenewLeases = false, TimeSpan? leaseAutoRenewInterval = null, Func<string> leasedByFactory = null)
        {
            ConfigureInLeaseMode(configurer, loggerFactory => new DbConnectionProvider(connectionStringOrConnectionStringName, loggerFactory), tableName, inputQueueName, leaseInterval, leaseTolerance, automaticallyRenewLeases, leaseAutoRenewInterval);
        }

        /// <summary>
        /// Configures Rebus to use SQL Server as its transport. The table specified by <paramref name="tableName"/> will be used to
        /// store messages, and the "queue" specified by <paramref name="inputQueueName"/> will be used when querying for messages.
        /// The message table will automatically be created if it does not exist.
        /// </summary>
        /// <param name="configurer">Static to extend</param>
        /// <param name="connectionFactory">Factory to provide a new connection</param>
        /// <param name="tableName">Name of the table to store messages in</param>
        /// <param name="inputQueueName">Name of the queue being services</param>
        /// <param name="leaseInterval">If <c>null</c> will default to <seealso cref="SqlServerLeaseTransport.DefaultLeaseTime"/>. Specifies how long a worker will request to keep a message. Higher values require less database communication but increase latency of a message being processed if a worker dies</param>
        /// <param name="leaseTolerance">If <c>null</c> defaults to <seealso cref="SqlServerLeaseTransport.DefaultLeaseTolerance"/>. Workers will wait for this amount of time to elapse, beyond the lease time, before they pick up an already leased message.</param>
        /// <param name="automaticallyRenewLeases">If <c>true</c> then workers will automatically renew the lease they have acquired whilst they're still processing the message. This will occur in accordance with <paramref name="leaseAutoRenewInterval"/></param>
        /// <param name="leaseAutoRenewInterval">If <c>null</c> defaults to <seealso cref="SqlServerLeaseTransport.DefaultLeaseAutomaticRenewal"/>. Specifies how frequently a lease will be renewed whilst the worker is processing a message. Lower values decrease the chance of other workers processing the same message but increase DB communication. A value 50% of <paramref name="leaseInterval"/> should be appropriate</param>
        /// <param name="leasedByFactory">If non-<c>null</c> a factory which returns a string identifying this worker when it leases a message. If <c>null></c> the current machine name is used</param>
        public static void UseSqlServerInLeaseMode(this StandardConfigurer<ITransport> configurer, Func<Task<IDbConnection>> connectionFactory, string tableName, string inputQueueName, TimeSpan? leaseInterval = null, TimeSpan? leaseTolerance = null, bool automaticallyRenewLeases = false, TimeSpan? leaseAutoRenewInterval = null, Func<string> leasedByFactory = null)
        {
            ConfigureInLeaseMode(configurer, loggerFactory => new DbConnectionFactoryProvider(connectionFactory, loggerFactory), tableName, inputQueueName, leaseInterval, leaseTolerance, automaticallyRenewLeases, leaseAutoRenewInterval);
        }

        /// <summary>
        /// Configures everything for a leased <seealso cref="SqlServerLeaseTransport"/>
        /// </summary>	
        static void ConfigureInLeaseMode(StandardConfigurer<ITransport> configurer, Func<IRebusLoggerFactory, IDbConnectionProvider> connectionProviderFactory, string tableName, string inputQueueName, TimeSpan? leaseInterval = null, TimeSpan? leaseTolerance = null, bool automaticallyRenewLeases = false, TimeSpan? leaseAutoRenewInterval = null, Func<string> leasedByFactory = null)
        {
            if (leasedByFactory == null)
            {
                leasedByFactory = () => Environment.MachineName;
            }

            Configure(configurer, connectionProviderFactory, tableName, inputQueueName, (context, provider, name, queueName) =>
            {
                var rebusLoggerFactory = context.Get<IRebusLoggerFactory>();
                var asyncTaskFactory = context.Get<IAsyncTaskFactory>();
                return new SqlServerLeaseTransport(
                    provider,
                    name,
                    inputQueueName,
                    rebusLoggerFactory,
                    asyncTaskFactory,
                    leaseInterval ?? SqlServerLeaseTransport.DefaultLeaseTime,
                    leaseTolerance ?? SqlServerLeaseTransport.DefaultLeaseTolerance, leasedByFactory,
                    automaticallyRenewLeases
                        ? (TimeSpan?)null
                        : leaseAutoRenewInterval ?? SqlServerLeaseTransport.DefaultLeaseAutomaticRenewal
                );
            });
        }

        /// <summary>
        /// Configures Rebus to use SQL Server to transport messages as a one-way client (i.e. will not be able to receive any messages).
        /// The table specified by <paramref name="tableName"/> will be used to store messages.
        /// The message table will automatically be created if it does not exist.
        /// </summary>
        public static void UseSqlServerAsOneWayClient(this StandardConfigurer<ITransport> configurer, Func<Task<IDbConnection>> connectionFactory, string tableName)
        {
			Configure(configurer, loggerFactory => new DbConnectionFactoryProvider(connectionFactory, loggerFactory), tableName, null);

            OneWayClientBackdoor.ConfigureOneWayClient(configurer);
        }

        /// <summary>
        /// Configures Rebus to use SQL Server to transport messages as a one-way client (i.e. will not be able to receive any messages).
        /// The table specified by <paramref name="tableName"/> will be used to store messages.
        /// The message table will automatically be created if it does not exist.
        /// </summary>
        public static void UseSqlServerAsOneWayClient(this StandardConfigurer<ITransport> configurer, string connectionStringOrConnectionStringName, string tableName)
        {
            Configure(configurer, loggerFactory => new DbConnectionProvider(connectionStringOrConnectionStringName, loggerFactory), tableName, null);

            OneWayClientBackdoor.ConfigureOneWayClient(configurer);
        }

        /// <summary>
        /// Configures Rebus to use SQL Server as its transport. The table specified by <paramref name="tableName"/> will be used to
        /// store messages, and the "queue" specified by <paramref name="inputQueueName"/> will be used when querying for messages.
        /// The message table will automatically be created if it does not exist.
        /// </summary>
        public static void UseSqlServer(this StandardConfigurer<ITransport> configurer, Func<Task<IDbConnection>> connectionFactory, string tableName, string inputQueueName)
        {
            Configure(configurer, loggerFactory => new DbConnectionFactoryProvider(connectionFactory, loggerFactory), tableName, inputQueueName);
        }

        /// <summary>
        /// Configures Rebus to use SQL Server as its transport. The table specified by <paramref name="tableName"/> will be used to
        /// store messages, and the "queue" specified by <paramref name="inputQueueName"/> will be used when querying for messages.
        /// The message table will automatically be created if it does not exist.
        /// </summary>
        public static void UseSqlServer(this StandardConfigurer<ITransport> configurer, string connectionStringOrConnectionStringName, string tableName, string inputQueueName)
        {
            Configure(configurer, loggerFactory => new DbConnectionProvider(connectionStringOrConnectionStringName, loggerFactory), tableName, inputQueueName);
        }


		delegate SqlServerTransport TransportFactoryDelegate(IResolutionContext context, IDbConnectionProvider connectionProvider, string tableName, string inputQueueName);

		/// <summary>
		/// Configures everything for a standard <seealso cref="SqlServerTransport"/>
		/// </summary>
		static void Configure(StandardConfigurer<ITransport> configurer, Func<IRebusLoggerFactory, IDbConnectionProvider> connectionProviderFactory, string tableName, string inputQueueName)
		{
			Configure(configurer, connectionProviderFactory, tableName, inputQueueName, (context, provider, name, queueName) => new SqlServerTransport(provider, name, queueName, context.Get<IRebusLoggerFactory>(), context.Get<IAsyncTaskFactory>()));
		}

		static void Configure(StandardConfigurer<ITransport> configurer, Func<IRebusLoggerFactory, IDbConnectionProvider> connectionProviderFactory, string tableName, string inputQueueName, TransportFactoryDelegate transportFactory)
        {
            configurer.Register(context =>
            {
                var rebusLoggerFactory = context.Get<IRebusLoggerFactory>();
                var connectionProvider = connectionProviderFactory(rebusLoggerFactory);
				var transport = transportFactory(context, connectionProvider, tableName, inputQueueName);
                transport.EnsureTableIsCreated();
                return transport;
            });

            configurer.OtherService<ITimeoutManager>().Register(c => new DisabledTimeoutManager());

            configurer.OtherService<IPipeline>().Decorate(c =>
            {
                var pipeline = c.Get<IPipeline>();

                return new PipelineStepRemover(pipeline)
                    .RemoveIncomingStep(s => s.GetType() == typeof(HandleDeferredMessagesStep));
            });

            configurer.OtherService<Options>().Decorate(c =>
            {
                var options = c.Get<Options>();

                if (string.IsNullOrWhiteSpace(options.ExternalTimeoutManagerAddressOrNull))
                {
                    options.ExternalTimeoutManagerAddressOrNull = SqlServerTransport.MagicExternalTimeoutManagerAddress;
                }

                return options;
            });
        }
    }
}