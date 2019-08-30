using System;
using System.Threading.Tasks;
using Rebus.SqlServer;
using Rebus.SqlServer.Transport;

namespace Rebus.Config
{
    /// <summary>
    /// Extends <seealso cref="SqlServerTransportOptions"/> with options specific to the <seealso cref="SqlServerLeaseTransport"/>
    /// </summary>
    public class SqlServerLeaseTransportOptions : SqlServerTransportOptions
    {
        /// <summary>
        /// Create an instance of the transport with a pre-created <seealso cref="DbConnectionProvider"/>
        /// </summary>
        public SqlServerLeaseTransportOptions(IDbConnectionProvider connectionProvider) : base(connectionProvider)
        {
        }

        /// <summary>
        /// Creates an instance of the transport connecting via <paramref name="connectionString"/>
        /// </summary>
        public SqlServerLeaseTransportOptions(string connectionString, bool enlistInAmbientTransaction = false) : base(connectionString, enlistInAmbientTransaction)
        {
        }

        /// <summary>
        /// Creates an instance of the transport with utilising an <seealso cref="IDbConnectionProvider"/> factory
        /// </summary>
        public SqlServerLeaseTransportOptions(Func<Task<IDbConnection>> connectionFactory) : base(connectionFactory)
        {
        }

        /// <summary>
        /// If <c>null</c> will default to <seealso cref="SqlServerLeaseTransport.DefaultLeaseTime"/>. Specifies how long a worker will request to keep a message. Higher values require less database communication but increase latency of a message being processed if a worker dies
        /// </summary>
        public TimeSpan? LeaseInterval { get; internal set; }

        /// <summary>
        /// If <c>null</c> will default to <seealso cref="SqlServerLeaseTransport.DefaultLeaseTime"/>. Specifies how long a worker will request to keep a message. Higher values require less database communication but increase latency of a message being processed if a worker dies
        /// </summary>
        public TimeSpan? LeaseTolerance { get; internal set; }

        /// <summary>
        /// If <c>true</c> then workers will automatically renew the lease they have acquired whilst they're still processing the message. This will occur in accordance with <seealso cref="LeaseAutoRenewInterval"/>
        /// </summary>
        public bool AutomaticallyRenewLeases { get; internal set; }

        /// <summary>
        /// If <c>null</c> defaults to <seealso cref="SqlServerLeaseTransport.DefaultLeaseAutomaticRenewal"/>. Specifies how frequently a lease will be renewed whilst the worker is processing a message. Lower values decrease the chance of other workers processing the same message but increase DB communication. A value 50% of <seealso cref="LeaseInterval"/> should be appropriate
        /// </summary>
        public TimeSpan? LeaseAutoRenewInterval { get; internal set; }

        /// <summary>
        /// If non-<c>null</c> a factory which returns a string identifying this worker when it leases a message. If <c>null></c> the current machine name is used
        /// </summary>
        public Func<string> LeasedByFactory { get; internal set; }
    }
}
