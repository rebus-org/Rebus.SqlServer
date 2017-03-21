// ReSharper disable CheckNamespace
#if NETSTANDARD1_6
namespace System.Configuration
{
    /// <summary>
    /// Configuration manager shim
    /// </summary>
    public static class ConfigurationManager
    {
        /// <summary>
        /// Gets connection strings
        /// </summary>
        public static ConnectionStringsIndexer ConnectionStrings => new ConnectionStringsIndexer();

        /// <summary>
        /// Gets the configuration element
        /// </summary>
        public class ConnectionStringsIndexer
        {
            /// <summary>
            /// Gets the connection string with the given <paramref name="name"/>
            /// </summary>
            public ConnectionStringSettings this[string name]
            {
                get
                {
                    var s = "";
                    return new ConnectionStringSettings(s);
                }
            }
        }

        /// <summary>
        /// Represents a connection string
        /// </summary>
        public class ConnectionStringSettings
        {
            internal ConnectionStringSettings(string connectionString)
            {
                ConnectionString = connectionString;
            }

            /// <summary>
            /// Gets the connection string to use
            /// </summary>
            public string ConnectionString { get; }
        }
    }
}
#endif
