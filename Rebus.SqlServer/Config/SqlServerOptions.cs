using System;
using Rebus.Injection;
using Rebus.SqlServer;

namespace Rebus.Config
{
    /// <summary>
    /// Options base class
    /// </summary>
    public abstract class SqlServerOptions
    {
        /// <summary>
        /// Connection provider used to create connections for the transport
        /// </summary>
        public Func<IResolutionContext, IDbConnectionProvider> ConnectionProviderFactory { get; protected set; }

        /// <summary>
        /// If <c>false</c> tables will not be created and must be created outside of Rebus
        /// </summary>
        public bool EnsureTablesAreCreated { get; internal set; } = true;
    }
}
