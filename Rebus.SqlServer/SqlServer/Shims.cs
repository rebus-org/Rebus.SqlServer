#if NETSTANDARD1_6
// ReSharper disable CheckNamespace
using System.Reflection;

namespace System
{
    static class ReflectionExtensions
    {
        public static PropertyInfo GetProperty(this Type type, string name)
        {
            return type.GetTypeInfo().GetProperty(name);
        }

        public static bool IsInstanceOfType(this Type type, object o)
        {
            return type.GetTypeInfo().IsInstanceOfType(o);
        }
    }
}

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
            public ConnectionStringSettings this[string name] => null; // no replacement for now
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