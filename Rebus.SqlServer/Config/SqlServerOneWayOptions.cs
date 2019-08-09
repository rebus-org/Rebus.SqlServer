using System;
using Rebus.SqlServer;

namespace Rebus.Config
{
    /// <summary>
    /// Options builder that can be used to customize the transport
    /// </summary>
    public class SqlServerOneWayOptions
    {
        readonly TimingConfiguration _timingConfiguration = new TimingConfiguration();

        /// <summary>
        /// Configures the transport to use the schema specified by <paramref name="schema"/>
        /// </summary>
        public SqlServerOneWayOptions WithSchema(string schema)
        {
            Schema = schema ?? throw new ArgumentNullException(nameof(schema));
            return this;
        }

        internal string Schema { get; set; } = "dbo";

        internal TimingConfiguration GetTimingConfiguration() => _timingConfiguration;
    }

    /// <summary>
    /// Options builder that can be used to customize the transport
    /// </summary>
    public class SqlServerOptions
    {
        readonly TimingConfiguration _timingConfiguration = new TimingConfiguration();

        /// <summary>
        /// Configures the transport to use the schema specified by <paramref name="schema"/>
        /// </summary>
        public SqlServerOptions WithSchema(string schema)
        {
            Schema = schema ?? throw new ArgumentNullException(nameof(schema));
            return this;
        }

        /// <summary>
        /// Sets the interval with which expired messages are cleaned up in the background. If the interval is zero or below, expired messages will not be cleaned up.
        /// </summary>
        public SqlServerOptions SetExpiredMessagesCleanupInterval(TimeSpan interval)
        {
            _timingConfiguration.ExpiredMessagesCleanupInterval = interval;
            return this;
        }

        internal string Schema { get; set; } = "dbo";

        internal TimingConfiguration GetTimingConfiguration() => _timingConfiguration;
    }
}