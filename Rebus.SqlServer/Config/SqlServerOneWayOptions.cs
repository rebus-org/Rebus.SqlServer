using System;

namespace Rebus.Config
{
    /// <summary>
    /// Options builder that can be used to customize the transport
    /// </summary>
    public class SqlServerOneWayOptions
    {
        /// <summary>
        /// Configures the transport to use the schema specified by <paramref name="schema"/>
        /// </summary>
        public SqlServerOneWayOptions WithSchema(string schema)
        {
            Schema = schema ?? throw new ArgumentNullException(nameof(schema));
            return this;
        }

        internal string Schema { get; set; } = "dbo";
    }

    /// <summary>
    /// Options builder that can be used to customize the transport
    /// </summary>
    public class SqlServerOptions
    {
        /// <summary>
        /// Configures the transport to use the schema specified by <paramref name="schema"/>
        /// </summary>
        public SqlServerOptions WithSchema(string schema)
        {
            Schema = schema ?? throw new ArgumentNullException(nameof(schema));
            return this;
        }

        internal string Schema { get; set; } = "dbo";
    }
}