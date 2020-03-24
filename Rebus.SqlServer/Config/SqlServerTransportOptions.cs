using System;
using System.Threading.Tasks;
using Rebus.Injection;
using Rebus.Logging;
using Rebus.SqlServer;
using Rebus.SqlServer.Transport;

namespace Rebus.Config
{
    /// <summary>
    /// Describes options used to configure the <seealso cref="SqlServerTransport"/>
    /// </summary>
    public class SqlServerTransportOptions
    {
        /// <summary>
        /// Create an instance of the transport with a pre-created <seealso cref="DbConnectionProvider"/>
        /// </summary>
        public SqlServerTransportOptions(IDbConnectionProvider connectionProvider)
        {
            ConnectionProviderFactory = (resolutionContext) => connectionProvider;
        }

        /// <summary>
        /// Creates an instance of the transport connecting via <paramref name="connectionString"/>
        /// </summary>
        public SqlServerTransportOptions(string connectionString, bool enlistInAmbientTransaction = false)
        {
            ConnectionProviderFactory = (resolutionContext) => new DbConnectionProvider(connectionString, resolutionContext.Get<IRebusLoggerFactory>(), enlistInAmbientTransaction);
        }

        /// <summary>
        /// Creates an instance of the transport with utilising an <seealso cref="IDbConnectionProvider"/> factory
        /// </summary>
        public SqlServerTransportOptions(Func<Task<IDbConnection>> connectionFactory)
        {
            ConnectionProviderFactory = (resolutionContext) => new DbConnectionFactoryProvider(connectionFactory, resolutionContext.Get<IRebusLoggerFactory>());
        }

        /// <summary>
        /// Connection provider used to create connections for the transport
        /// </summary>
        public Func<IResolutionContext, IDbConnectionProvider> ConnectionProviderFactory { get; }

        /// <summary>
        /// Name of the input queue to process. If <c>null</c> or whitespace the transport will be configured in one way mode (send only)
        /// </summary>
        public string InputQueueName { get; internal set; }

        /// <summary>
        /// If <c>true</c> the transport is configured in one way mode
        /// </summary>
        public bool IsOneWayQueue => InputQueueName == null;

        /// <summary>
        /// If <c>false</c> tables will not be created and must be created outside of Rebus
        /// </summary>
        public bool EnsureTablesAreCreated { get; internal set; } = true;

        /// <summary>
        /// If true, the input queue table will be automatically dropped on transport disposal
        /// </summary>
        public bool AutoDeleteQueue { get; internal set; } = false;

        /// <summary>
        /// Gets the delay between executions of the background cleanup task
        /// </summary>
        internal TimeSpan? ExpiredMessagesCleanupInterval { get; set; }
    }
}
