using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;

namespace Rebus.SqlServer.Tests.Extensions
{
    public static class ConnectionExtensions
    {
        public static IEnumerable<T> Query<T>(this SqlConnection connection, string query)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = query;

#if NET45
                var properties = typeof(T).GetProperties().Select(p => p.Name).ToArray();
#else
                var properties = typeof(T).GetTypeInfo().GetProperties().Select(p => p.Name).ToArray();
#endif

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var instance = Activator.CreateInstance(typeof(T));

                        foreach (var name in properties)
                        {
                            var ordinal = reader.GetOrdinal(name);
                            var value = reader.GetValue(ordinal);

#if NET45
                            instance.GetType().GetProperty(name).SetValue(instance, value);
#else
                            ReflectionExtensions.GetProperty(instance.GetType(), name).SetValue(instance, value);
#endif
                        }

                        yield return (T)instance;
                    }
                }
            }
        }
    }
}