using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.SqlClient;

namespace Rebus.SqlServer.Tests.Extensions
{
    public static class ConnectionExtensions
    {
        public static IEnumerable<T> Query<T>(this SqlConnection connection, string query)
        {
            using var command = connection.CreateCommand();
            command.CommandText = query;

            var properties = typeof(T).GetProperties().Select(p => p.Name).ToArray();

            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                var instance = Activator.CreateInstance(typeof(T));

                foreach (var name in properties)
                {
                    var ordinal = reader.GetOrdinal(name);
                    var value = reader.GetValue(ordinal);

                    instance.GetType().GetProperty(name).SetValue(instance, value);
                }

                yield return (T)instance;
            }
        }
    }
}
