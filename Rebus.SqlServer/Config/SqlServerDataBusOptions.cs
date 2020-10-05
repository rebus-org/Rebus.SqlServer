using System;
using Rebus.Injection;
using Rebus.Logging;
using Rebus.SqlServer;
using Rebus.SqlServer.DataBus;

namespace Rebus.Config
{
    /// <summary>
    /// Describes options used to configure <seealso cref="SqlServerDataBusStorage"/>
    /// </summary>
    public class SqlServerDataBusOptions : SqlServerOptions
    {
        /// <summary>
        /// Creates the options with the given cnnection provider factory
        /// </summary>
        public SqlServerDataBusOptions(Func<IResolutionContext, IDbConnectionProvider> connectionProviderFactory)
        {
            ConnectionProviderFactory = connectionProviderFactory ?? throw new ArgumentNullException(nameof(connectionProviderFactory));
        }

        /// <summary>
        /// Creates the options with the given <paramref name="connectionProvider"/>
        /// </summary>
        public SqlServerDataBusOptions(IDbConnectionProvider connectionProvider)
        {
            if (connectionProvider == null) throw new ArgumentNullException(nameof(connectionProvider));

            ConnectionProviderFactory = context => connectionProvider;
        }

        /// <summary>
        /// Creates an instance of the options via <paramref name="connectionString"/>
        /// </summary>
        public SqlServerDataBusOptions(string connectionString, bool enlistInAmbientTransactions = false)
        {
            ConnectionProviderFactory = context => new DbConnectionProvider(connectionString, context.Get<IRebusLoggerFactory>(), enlistInAmbientTransactions);
        }

        /// <summary>
        /// Configures the commanbd timeout
        /// </summary>
        public TimeSpan CommandTimeout { get; set; } = TimeSpan.FromSeconds(240);
    }
}
