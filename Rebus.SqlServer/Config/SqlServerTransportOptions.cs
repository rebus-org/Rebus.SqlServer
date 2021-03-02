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
    public class SqlServerTransportOptions : SqlServerOptions
    {
        /// <summary>
        /// Create an instance of the transport with a pre-created <seealso cref="DbConnectionProvider"/>
        /// </summary>
        public SqlServerTransportOptions(IDbConnectionProvider connectionProvider)
        {
            ConnectionProviderFactory = (resolutionContext) => connectionProvider;
        }

        /// <summary>
        /// Create an instance of the transport with a <paramref name="connectionProviderFactory"/> that can use the <see cref="IResolutionContext"/> to look up things
        /// </summary>
        public SqlServerTransportOptions(Func<IResolutionContext, IDbConnectionProvider> connectionProviderFactory)
        {
            ConnectionProviderFactory = connectionProviderFactory ?? throw new ArgumentNullException(nameof(connectionProviderFactory));
        }

        /// <summary>
        /// Creates an instance of the transport connecting via <paramref name="connectionString"/>
        /// </summary>
        public SqlServerTransportOptions(string connectionString, bool enlistInAmbientTransaction = false)
        {
            ConnectionProviderFactory = resolutionContext => new DbConnectionProvider(connectionString, resolutionContext.Get<IRebusLoggerFactory>(), enlistInAmbientTransaction);
        }

        /// <summary>
        /// Creates an instance of the transport with utilising an <seealso cref="IDbConnectionProvider"/> factory
        /// </summary>
        public SqlServerTransportOptions(Func<Task<IDbConnection>> connectionFactory)
        {
            ConnectionProviderFactory = resolutionContext => new DbConnectionFactoryProvider(connectionFactory);
        }

        /// <summary>
        /// Disables the SQL transport's built-in ability to delay message delivery. This can be done if ther requirements for delayed messages
        /// exceeds what is convenient, as delayed messages will be sitting in the recipient's table until it is time to be consumed.
        /// </summary>
        public SqlServerTransportOptions DisableNativeTimeoutManager()
        {
            NativeTimeoutManagerDisabled = true;
            return this;
        }

        /// <summary>
        /// Name of the input queue to process. If <c>null</c> or whitespace the transport will be configured in one way mode (send only)
        /// </summary>
        public string InputQueueName { get; internal set; }

        /// <summary>
        /// If true, the input queue table will be automatically dropped on transport disposal
        /// </summary>
        public bool AutoDeleteQueue { get; internal set; } = false;

        /// <summary>
        /// If <c>true</c> the transport is configured in one way mode
        /// </summary>
        internal bool IsOneWayClient => InputQueueName == null;

        /// <summary>
        /// Gets the delay between executions of the background cleanup task
        /// </summary>
        internal TimeSpan? ExpiredMessagesCleanupInterval { get; set; }

        internal bool NativeTimeoutManagerDisabled { get; set; }
    }
}
