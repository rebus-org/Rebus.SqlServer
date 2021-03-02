using System;
using System.Threading.Tasks;
using Rebus.Injection;
using Rebus.Logging;
using Rebus.Pipeline;
using Rebus.Pipeline.Receive;
using Rebus.SqlServer;
using Rebus.SqlServer.Transport;
using Rebus.Threading;
using Rebus.Time;
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
        /// Configures Rebus to use SQL Server as its transport. Unlike the <c>UseSqlServer</c> calls the leased version of the SQL 
        /// Server transport does not hold a transaction open for the entire duration of the message handling. Instead it marks a
        /// message as being "leased" for a period of time. If the lease has expired then a worker is permitted to acquire the that
        /// message again and try reprocessing
        /// </summary>
        /// <param name="configurer">Static to extend</param>
        /// <param name="transportOptions">Options controlling the transport setup</param>
        /// <param name="inputQueueName">Queue name to process messages from</param>
        public static SqlServerLeaseTransportOptions UseSqlServerInLeaseMode(this StandardConfigurer<ITransport> configurer, SqlServerLeaseTransportOptions transportOptions, string inputQueueName)
        {
            return Configure(
                    configurer,
                    (context, provider, inputQueue) =>
                    {
                        if (transportOptions.LeasedByFactory == null)
                        {
                            transportOptions.SetLeasedByFactory(() => Environment.MachineName);
                        }

                        return new SqlServerLeaseTransport(
                            provider,
                            transportOptions.InputQueueName,
                            context.Get<IRebusLoggerFactory>(),
                            context.Get<IAsyncTaskFactory>(),
                            context.Get<IRebusTime>(),
                            transportOptions.LeaseInterval ?? SqlServerLeaseTransport.DefaultLeaseTime,
                            transportOptions.LeaseTolerance ?? SqlServerLeaseTransport.DefaultLeaseTolerance,
                            transportOptions.LeasedByFactory,
                            transportOptions

                        );
                    },
                    transportOptions
                )
                .ReadFrom(inputQueueName);
        }

        /// <summary>
        /// Configures Rebus to use SQL Server as its transport in "one-way client mode" (i.e. as a send only endpoint). Unlike the <c>UseSqlServer</c> calls the leased version of the SQL 
        /// Server transport does not hold a transaction open for the entire duration of the message handling. Instead it marks a
        /// message as being "leased" for a period of time. If the lease has expired then a worker is permitted to acquire the that
        /// message again and try reprocessing
        /// </summary>
        /// <param name="configurer">Static to extend</param>
        /// <param name="transportOptions">Options controlling the transport setup</param>
        public static SqlServerLeaseTransportOptions UseSqlServerInLeaseModeAsOneWayClient(this StandardConfigurer<ITransport> configurer, SqlServerLeaseTransportOptions transportOptions)
        {
            return Configure(
                    configurer,
                    (context, provider, inputQueue) =>
                    {
                        if (transportOptions.LeasedByFactory == null)
                        {
                            transportOptions.SetLeasedByFactory(() => Environment.MachineName);
                        }

                        return new SqlServerLeaseTransport(
                            provider,
                            transportOptions.InputQueueName,
                            context.Get<IRebusLoggerFactory>(),
                            context.Get<IAsyncTaskFactory>(),
                            context.Get<IRebusTime>(),
                            transportOptions.LeaseInterval ?? SqlServerLeaseTransport.DefaultLeaseTime,
                            transportOptions.LeaseTolerance ?? SqlServerLeaseTransport.DefaultLeaseTolerance,
                            transportOptions.LeasedByFactory,
                            transportOptions
                        );
                    },
                    transportOptions
                )
                .AsOneWayClient();
        }

        /// <summary>
        /// Configures Rebus to use SQL Server as its transport
        /// </summary>
        /// <param name="configurer">Static to extend</param>
        /// <param name="transportOptions">Options controlling the transport setup</param>
        /// <param name="inputQueueName">Queue name to process messages from</param>
        public static SqlServerTransportOptions UseSqlServer(this StandardConfigurer<ITransport> configurer, SqlServerTransportOptions transportOptions, string inputQueueName)
        {
            return Configure(
                    configurer,
                    (context, provider, inputQueue) => new SqlServerTransport(provider, inputQueue, context.Get<IRebusLoggerFactory>(), context.Get<IAsyncTaskFactory>(), context.Get<IRebusTime>(), transportOptions),
                    transportOptions
                )
                .ReadFrom(inputQueueName);
        }

        /// <summary>
        /// Configures Rebus to use SQLServer as its transport in "one-way client mode" (i.e. as a send-only endpoint). 
        /// </summary>
        /// <param name="configurer"></param>
        /// <param name="transportOptions"></param>
        /// <returns></returns>
        public static SqlServerTransportOptions UseSqlServerAsOneWayClient(this StandardConfigurer<ITransport> configurer, SqlServerTransportOptions transportOptions)
        {
            return Configure(
                    configurer,
                    (context, provider, inputQueue) => new SqlServerTransport(provider, inputQueue, context.Get<IRebusLoggerFactory>(), context.Get<IAsyncTaskFactory>(), context.Get<IRebusTime>(), transportOptions),
                    transportOptions
                )
                .AsOneWayClient();
        }

        /// <summary>
        /// Configures Rebus to use SQL Server as its transport (in "one-way client mode", i.e. as a send-only endpoint). Unlike the <c>UseSqlServer</c> calls the leased version of the SQL 
        /// Server transport does not hold a transaction open for the entire duration of the message handling. Instead it marks a
        /// message as being "leased" for a period of time. If the lease has expired then a worker is permitted to acquire the that
        /// message again and try reprocessing
        /// </summary>
        /// <param name="configurer">Static to extend</param>
        /// <param name="connectionString">Connection string</param>
        /// <param name="leaseInterval">If <c>null</c> will default to <seealso cref="SqlServerLeaseTransport.DefaultLeaseTime"/>. Specifies how long a worker will request to keep a message. Higher values require less database communication but increase latency of a message being processed if a worker dies</param>
        /// <param name="leaseTolerance">If <c>null</c> defaults to <seealso cref="SqlServerLeaseTransport.DefaultLeaseTolerance"/>. Workers will wait for this amount of time to elapse, beyond the lease time, before they pick up an already leased message.</param>
        /// <param name="automaticallyRenewLeases">If <c>true</c> then workers will automatically renew the lease they have acquired whilst they're still processing the message. This will occur in accordance with <paramref name="leaseAutoRenewInterval"/></param>
        /// <param name="leaseAutoRenewInterval">If <c>null</c> defaults to <seealso cref="SqlServerLeaseTransport.DefaultLeaseAutomaticRenewal"/>. Specifies how frequently a lease will be renewed whilst the worker is processing a message. Lower values decrease the chance of other workers processing the same message but increase DB communication. A value 50% of <paramref name="leaseInterval"/> should be appropriate</param>
        /// <param name="leasedByFactory">If non-<c>null</c> a factory which returns a string identifying this worker when it leases a message. If <c>null></c> the current machine name is used</param>
        /// <param name="enlistInAmbientTransaction">If <c>true</c> the connection will be enlisted in the ambient transaction if it exists, else it will create an SqlTransaction and enlist in it</param>
        /// <param name="ensureTablesAreCreated">If <c>true</c> tables for the queue will be created at run time. This means the connection provided to the transport must have schema modification rights. If <c>false</c> tables must be created externally before running</param>
        [Obsolete("Will be removed in a future version use " + nameof(UseSqlServerInLeaseMode) + " with a " + nameof(SqlServerLeaseTransportOptions) + " instead.")]
        public static void UseSqlServerInLeaseModeAsOneWayClient(this StandardConfigurer<ITransport> configurer, string connectionString, TimeSpan? leaseInterval = null, TimeSpan? leaseTolerance = null, bool automaticallyRenewLeases = false, TimeSpan? leaseAutoRenewInterval = null, Func<string> leasedByFactory = null, bool enlistInAmbientTransaction = false, bool ensureTablesAreCreated = true)
        {
            configurer.UseSqlServerInLeaseModeAsOneWayClient(new SqlServerLeaseTransportOptions(connectionString, enlistInAmbientTransaction))
                .SetEnsureTablesAreCreated(ensureTablesAreCreated)
                .SetLeaseInterval(leaseAutoRenewInterval)
                .SetLeaseTolerance(leaseTolerance)
                .SetAutomaticLeaseRenewal(automaticallyRenewLeases, leaseAutoRenewInterval)
                .SetLeasedByFactory(leasedByFactory);
        }

        /// <summary>
        /// Configures Rebus to use SQL Server as its transport (in "one-way client mode", i.e. as a send-only endpoint). The message table will automatically be created if it does not exist.
        /// </summary>
        /// <param name="configurer">Static to extend</param>
        /// <param name="connectionFactory">Factory to provide a new connection</param>
        /// <param name="leaseInterval">If <c>null</c> will default to <seealso cref="SqlServerLeaseTransport.DefaultLeaseTime"/>. Specifies how long a worker will request to keep a message. Higher values require less database communication but increase latency of a message being processed if a worker dies</param>
        /// <param name="leaseTolerance">If <c>null</c> defaults to <seealso cref="SqlServerLeaseTransport.DefaultLeaseTolerance"/>. Workers will wait for this amount of time to elapse, beyond the lease time, before they pick up an already leased message.</param>
        /// <param name="automaticallyRenewLeases">If <c>true</c> then workers will automatically renew the lease they have acquired whilst they're still processing the message. This will occur in accordance with <paramref name="leaseAutoRenewInterval"/></param>
        /// <param name="leaseAutoRenewInterval">If <c>null</c> defaults to <seealso cref="SqlServerLeaseTransport.DefaultLeaseAutomaticRenewal"/>. Specifies how frequently a lease will be renewed whilst the worker is processing a message. Lower values decrease the chance of other workers processing the same message but increase DB communication. A value 50% of <paramref name="leaseInterval"/> should be appropriate</param>
        /// <param name="leasedByFactory">If non-<c>null</c> a factory which returns a string identifying this worker when it leases a message. If <c>null></c> the current machine name is used</param>
        [Obsolete("Will be removed in a future version use " + nameof(UseSqlServerInLeaseMode) + " with a " + nameof(SqlServerLeaseTransportOptions) + " instead.")]
        public static void UseSqlServerInLeaseModeAsOneWayClient(this StandardConfigurer<ITransport> configurer, Func<Task<IDbConnection>> connectionFactory, TimeSpan? leaseInterval = null, TimeSpan? leaseTolerance = null, bool automaticallyRenewLeases = false, TimeSpan? leaseAutoRenewInterval = null, Func<string> leasedByFactory = null)
        {
            configurer.UseSqlServerInLeaseModeAsOneWayClient(new SqlServerLeaseTransportOptions(connectionFactory))
                .SetLeaseInterval(leaseInterval)
                .SetLeaseTolerance(leaseTolerance)
                .SetAutomaticLeaseRenewal(automaticallyRenewLeases, leaseAutoRenewInterval)
                .SetLeasedByFactory(leasedByFactory);
        }

        /// <summary>
        /// Configures Rebus to use SQL Server as its transport. Unlike the <c>UseSqlServer</c> calls the leased version of the SQL 
        /// Server transport does not hold a transaction open for the entire duration of the message handling. Instead it marks a
        /// message as being "leased" for a period of time. If the lease has expired then a worker is permitted to acquire the that
        /// message again and try reprocessing
        /// </summary>
        /// <param name="configurer">Static to extend</param>
        /// <param name="connectionString">Connection string</param>
        /// <param name="inputQueueName">Name of the queue, which must be a valid table table in SQL Server</param>
        /// <param name="leaseInterval">If <c>null</c> will default to <seealso cref="SqlServerLeaseTransport.DefaultLeaseTime"/>. Specifies how long a worker will request to keep a message. Higher values require less database communication but increase latency of a message being processed if a worker dies</param>
        /// <param name="leaseTolerance">If <c>null</c> defaults to <seealso cref="SqlServerLeaseTransport.DefaultLeaseTolerance"/>. Workers will wait for this amount of time to elapse, beyond the lease time, before they pick up an already leased message.</param>
        /// <param name="automaticallyRenewLeases">If <c>true</c> then workers will automatically renew the lease they have acquired whilst they're still processing the message. This will occur in accordance with <paramref name="leaseAutoRenewInterval"/></param>
        /// <param name="leaseAutoRenewInterval">If <c>null</c> defaults to <seealso cref="SqlServerLeaseTransport.DefaultLeaseAutomaticRenewal"/>. Specifies how frequently a lease will be renewed whilst the worker is processing a message. Lower values decrease the chance of other workers processing the same message but increase DB communication. A value 50% of <paramref name="leaseInterval"/> should be appropriate</param>
        /// <param name="leasedByFactory">If non-<c>null</c> a factory which returns a string identifying this worker when it leases a message. If <c>null></c> the current machine name is used</param>
        /// <param name="enlistInAmbientTransaction">If <c>true</c> the connection will be enlisted in the ambient transaction if it exists, else it will create an SqlTransaction and enlist in it</param>
        /// <param name="ensureTablesAreCreated">If <c>true</c> tables for the queue will be created at run time. This means the connection provided to the transport must have schema modification rights. If <c>false</c> tables must be created externally before running</param>
        [Obsolete("Will be removed in a future version use " + nameof(UseSqlServerInLeaseMode) + " with a " + nameof(SqlServerLeaseTransportOptions) + " instead.")]
        public static void UseSqlServerInLeaseMode(this StandardConfigurer<ITransport> configurer, string connectionString, string inputQueueName, TimeSpan? leaseInterval = null, TimeSpan? leaseTolerance = null, bool automaticallyRenewLeases = false, TimeSpan? leaseAutoRenewInterval = null, Func<string> leasedByFactory = null, bool enlistInAmbientTransaction = false, bool ensureTablesAreCreated = true)
        {
            configurer.UseSqlServerInLeaseMode(new SqlServerLeaseTransportOptions(connectionString, enlistInAmbientTransaction), inputQueueName)
                .SetEnsureTablesAreCreated(ensureTablesAreCreated)
                .SetLeaseInterval(leaseInterval)
                .SetLeaseTolerance(leaseTolerance)
                .SetAutomaticLeaseRenewal(automaticallyRenewLeases, leaseAutoRenewInterval)
                .SetLeasedByFactory(leasedByFactory);
        }

        /// <summary>
        /// Configures Rebus to use SQL Server as its transport. The "queue" specified by <paramref name="inputQueueName"/> will be used when querying for messages.
        /// The message table will automatically be created if it does not exist.
        /// </summary>
        /// <param name="configurer">Static to extend</param>
        /// <param name="connectionFactory">Factory to provide a new connection</param>
        /// <param name="inputQueueName">Name of the queue, which must be a valid table table in SQL Server</param>
        /// <param name="leaseInterval">If <c>null</c> will default to <seealso cref="SqlServerLeaseTransport.DefaultLeaseTime"/>. Specifies how long a worker will request to keep a message. Higher values require less database communication but increase latency of a message being processed if a worker dies</param>
        /// <param name="leaseTolerance">If <c>null</c> defaults to <seealso cref="SqlServerLeaseTransport.DefaultLeaseTolerance"/>. Workers will wait for this amount of time to elapse, beyond the lease time, before they pick up an already leased message.</param>
        /// <param name="automaticallyRenewLeases">If <c>true</c> then workers will automatically renew the lease they have acquired whilst they're still processing the message. This will occur in accordance with <paramref name="leaseAutoRenewInterval"/></param>
        /// <param name="leaseAutoRenewInterval">If <c>null</c> defaults to <seealso cref="SqlServerLeaseTransport.DefaultLeaseAutomaticRenewal"/>. Specifies how frequently a lease will be renewed whilst the worker is processing a message. Lower values decrease the chance of other workers processing the same message but increase DB communication. A value 50% of <paramref name="leaseInterval"/> should be appropriate</param>
        /// <param name="leasedByFactory">If non-<c>null</c> a factory which returns a string identifying this worker when it leases a message. If <c>null></c> the current machine name is used</param>
        /// <param name="ensureTablesAreCreated">If <c>true</c> tables for the queue will be created at run time. This means the connection provided to the transport must have schema modification rights. If <c>false</c> tables must be created externally before running</param>
        [Obsolete("Will be removed in a future version use " + nameof(UseSqlServerInLeaseMode) + " with a " + nameof(SqlServerLeaseTransportOptions) + " instead.")]
        public static void UseSqlServerInLeaseMode(this StandardConfigurer<ITransport> configurer, Func<Task<IDbConnection>> connectionFactory, string inputQueueName, TimeSpan? leaseInterval = null, TimeSpan? leaseTolerance = null, bool automaticallyRenewLeases = false, TimeSpan? leaseAutoRenewInterval = null, Func<string> leasedByFactory = null, bool ensureTablesAreCreated = true)
        {
            configurer.UseSqlServerInLeaseMode(new SqlServerLeaseTransportOptions(connectionFactory), inputQueueName)
                .SetEnsureTablesAreCreated(ensureTablesAreCreated)
                .SetLeaseInterval(leaseInterval)
                .SetLeaseTolerance(leaseTolerance)
                .SetAutomaticLeaseRenewal(automaticallyRenewLeases, leaseAutoRenewInterval)
                .SetLeasedByFactory(leasedByFactory);
        }

        /// <summary>
        /// Configures Rebus to use SQL Server to transport messages as a one-way client (i.e. will not be able to receive any messages).
        /// The message table will automatically be created if it does not exist.
        /// </summary>
        [Obsolete("Will be removed in a future version use " + nameof(UseSqlServer) + " with a " + nameof(SqlServerTransport) + " instead.")]
        public static void UseSqlServerAsOneWayClient(this StandardConfigurer<ITransport> configurer, Func<Task<IDbConnection>> connectionFactory)
        {
            configurer.UseSqlServerAsOneWayClient(new SqlServerTransportOptions(connectionFactory));
        }

        /// <summary>
        /// Configures Rebus to use SQL Server to transport messages as a one-way client (i.e. will not be able to receive any messages).
        /// The message table will automatically be created if it does not exist.
        /// </summary>
        [Obsolete("Will be removed in a future version use " + nameof(UseSqlServer) + " with a " + nameof(SqlServerTransport) + " instead.")]
        public static void UseSqlServerAsOneWayClient(this StandardConfigurer<ITransport> configurer, string connectionString, bool enlistInAmbientTransaction = false)
        {
            configurer.UseSqlServerAsOneWayClient(new SqlServerTransportOptions(connectionString, enlistInAmbientTransaction))
                .AsOneWayClient();
        }

        /// <summary>
        /// Configures Rebus to use SQL Server as its transport. The "queue" specified by <paramref name="inputQueueName"/> will be used when querying for messages.
        /// The message table will automatically be created if it does not exist.
        /// </summary>
        [Obsolete("Will be removed in a future version use " + nameof(UseSqlServer) + " with a " + nameof(SqlServerTransport) + " instead.")]
        public static void UseSqlServer(this StandardConfigurer<ITransport> configurer, Func<Task<IDbConnection>> connectionFactory, string inputQueueName, bool ensureTablesAreCreated = true)
        {
            configurer.UseSqlServer(new SqlServerTransportOptions(connectionFactory), inputQueueName)
                .ReadFrom(inputQueueName)
                .SetEnsureTablesAreCreated(ensureTablesAreCreated);
        }

        /// <summary>
        /// Configures Rebus to use SQL Server as its transport. The "queue" specified by <paramref name="inputQueueName"/> will be used when querying for messages.
        /// The message table will automatically be created if it does not exist.
        /// </summary>
        [Obsolete("Will be removed in a future version use " + nameof(UseSqlServer) + " with a " + nameof(SqlServerTransport) + " instead.")]
        public static void UseSqlServer(this StandardConfigurer<ITransport> configurer, string connectionString, string inputQueueName, bool enlistInAmbientTransaction = false, bool ensureTablesAreCreated = true)
        {
            configurer.UseSqlServer(new SqlServerTransportOptions(connectionString, enlistInAmbientTransaction), inputQueueName)
                .ReadFrom(inputQueueName)
                .SetEnsureTablesAreCreated(ensureTablesAreCreated);
        }

        delegate SqlServerTransport TransportFactoryDelegate(IResolutionContext context, IDbConnectionProvider connectionProvider, string inputQueueName);

        static TTransportOptions Configure<TTransportOptions>(StandardConfigurer<ITransport> configurer, TransportFactoryDelegate transportFactory, TTransportOptions transportOptions) where TTransportOptions : SqlServerTransportOptions
        {
            configurer.Register(context =>
                {
                    if (transportOptions.IsOneWayClient)
                    {
                        OneWayClientBackdoor.ConfigureOneWayClient(configurer);
                    }

                    var connectionProvider = transportOptions.ConnectionProviderFactory(context);
                    var transport = transportFactory(context, connectionProvider, transportOptions.InputQueueName);
                    if ((transportOptions.InputQueueName != null) && (transportOptions.EnsureTablesAreCreated == true))
                    {
                        transport.EnsureTableIsCreated();
                    }

                    return transport;
                }
            );

            configurer.OtherService<Options>().Decorate(c =>
            {
                var options = c.Get<Options>();

                // if the transport is a one-way client and no external timeout manager has been configured, set the 
                // external timeout manager's address to this magic string, which we'll detect later on
                if (transportOptions.IsOneWayClient && string.IsNullOrWhiteSpace(options.ExternalTimeoutManagerAddressOrNull))
                {
                    options.ExternalTimeoutManagerAddressOrNull = SqlServerTransport.MagicExternalTimeoutManagerAddress;
                }

                return options;
            });

            return transportOptions;
        }
    }
}
