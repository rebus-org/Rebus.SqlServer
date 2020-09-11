using System;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.Linq;
using Microsoft.Data.SqlClient;

namespace Rebus.SqlServer
{
    /// <summary>
    /// Wraps some nice extension methods for <see cref="SqlConnection"/> that makes it easy e.g. to query the schema
    /// </summary>
    static class SqlServerMagic
    {
        /// <summary>
        /// Error code that is emitted on PK violations
        /// </summary>
        public const int PrimaryKeyViolationNumber = 2627;

        /// <summary>
        /// Error code that is emitted when something does not exist or the login's permissions do not allow the client to see it
        /// </summary>
        public const int ObjectDoesNotExistOrNoPermission = 3701;

        /// <summary>
        /// Gets the names of all tables in the current database
        /// </summary>
        public static List<TableName> GetTableNames(this SqlConnection connection, SqlTransaction transaction = null)
        {
            return GetNamesFrom(connection, transaction, "INFORMATION_SCHEMA.TABLES", new []{ "TABLE_SCHEMA", "TABLE_NAME" })
                .Select(x => new TableName((string)x.TABLE_SCHEMA, (string)x.TABLE_NAME))
                .ToList();
        }

        /// <summary>
        /// Gets the names of all indexes in the current database
        /// </summary>
        public static List<string> GetIndexNames(this SqlConnection connection, SqlTransaction transaction = null)
        {
            return GetNamesFrom(connection, transaction, "sys.indexes", new []{ "name" }).Select(x => (string)x.name).ToList();
        }

        /// <summary>
        /// Gets the names of all tables in the current database
        /// </summary>
        public static Dictionary<string, SqlDbType> GetColumns(this SqlConnection connection, string schema, string tableName, SqlTransaction transaction = null)
        {
            var results = new Dictionary<string, SqlDbType>();

            using (var command = connection.CreateCommand())
            {
                if (transaction != null)
                {
                    command.Transaction = transaction;
                }

                command.CommandText = $"SELECT [COLUMN_NAME] AS 'name', [DATA_TYPE] AS 'type' FROM [INFORMATION_SCHEMA].[COLUMNS] WHERE [TABLE_SCHEMA] = '{schema}' AND [TABLE_NAME] = '{tableName}'";

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var name = (string)reader["name"];
                        var typeString = (string)reader["type"];
                        var type = GetDbType(typeString);

                        results[name] = type;
                    }
                }
            }

            return results;
        }

        static SqlDbType GetDbType(string typeString)
        {
            try
            {
                return (SqlDbType)Enum.Parse(typeof(SqlDbType), typeString, true);
            }
            catch (Exception exception)
            {
                throw new FormatException($"Could not parse '{typeString}' into {typeof(SqlDbType)}", exception);
            }
        }

        static List<dynamic> GetNamesFrom(SqlConnection connection, SqlTransaction transaction, string systemTableName, string[] columnNames)
        {
            var names = new List<dynamic>();

            using (var command = connection.CreateCommand())
            {
                if (transaction != null)
                {
                    command.Transaction = transaction;
                }

                command.CommandText = $"SELECT {string.Join(",", columnNames)} FROM {systemTableName}";

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        dynamic obj = new ExpandoObject();
                        foreach (var columnName in columnNames)
                        {
                            ((IDictionary<string, object>)obj)[columnName] = reader[columnName].ToString();
                        }

                        names.Add(obj);
                    }
                }
            }

            return names;
        }
    }
}
