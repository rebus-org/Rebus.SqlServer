using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Rebus.Bus;
using Rebus.Exceptions;
using Rebus.Logging;
using Rebus.Subscriptions;

namespace Rebus.SqlServer.Subscriptions
{
    /// <summary>
    /// Implementation of <see cref="ISubscriptionStorage"/> that persists subscriptions in a table in SQL Server
    /// </summary>
    public class SqlServerSubscriptionStorage : ISubscriptionStorage, IInitializable
    {
        readonly IDbConnectionProvider _connectionProvider;
        readonly TableName _tableName;
        readonly ILog _log;

        int _topicLength = 200;
        int _addressLength = 200;

        /// <summary>
        /// Constructs the storage using the specified connection provider and table to store its subscriptions. If the subscription
        /// storage is shared by all subscribers and publishers, the <paramref name="isCentralized"/> parameter can be set to true
        /// in order to subscribe/unsubscribe directly instead of sending subscription/unsubscription requests
        /// </summary>
        public SqlServerSubscriptionStorage(IDbConnectionProvider connectionProvider, string tableName, bool isCentralized, IRebusLoggerFactory rebusLoggerFactory)
        {
            _connectionProvider = connectionProvider ?? throw new ArgumentNullException(nameof(connectionProvider));
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));

            IsCentralized = isCentralized;

            _log = rebusLoggerFactory.GetLogger<SqlServerSubscriptionStorage>();
            _tableName = TableName.Parse(tableName);
        }

        /// <summary>
        /// Initializes the subscription storage by reading the lengths of the [topic] and [address] columns from SQL Server
        /// </summary>
        public void Initialize()
        {
            AsyncHelpers.RunSync(async () =>
            {
                try
                {
                    using (var connection = await _connectionProvider.GetConnection())
                    {
                        _topicLength = GetColumnWidth("topic", connection);
                        _addressLength = GetColumnWidth("address", connection);
                    }
                }
                catch (Exception exception)
                {
                    throw new RebusApplicationException(exception, "Error during schema reflection");
                }
            });
        }

        int GetColumnWidth(string columnName, IDbConnection connection)
        {
            var sql = $@"
SELECT 
    CHARACTER_MAXIMUM_LENGTH

FROM INFORMATION_SCHEMA.COLUMNS

WHERE 
    TABLE_SCHEMA = '{_tableName.Schema}' 
    AND TABLE_NAME = '{_tableName.Name}'
    AND COLUMN_NAME = '{columnName}'
";

            try
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = sql;
                    return (int)command.ExecuteScalar();
                }
            }
            catch (Exception exception)
            {
                throw new RebusApplicationException(exception, $"Could not get size of the [{columnName}] column from {_tableName} - executed SQL: '{sql}'");
            }
        }

        /// <summary>
        /// Creates the subscriptions table if necessary
        /// </summary>
        public void EnsureTableIsCreated()
        {
            try
            {
                AsyncHelpers.RunSync(EnsureTableIsCreatedAsync);
            }
            catch
            {
                // if it failed because of a collision between another thread doing the same thing, just try again once:
                AsyncHelpers.RunSync(EnsureTableIsCreatedAsync);
            }
        }

        async Task EnsureTableIsCreatedAsync()
        {
            using (var connection = await _connectionProvider.GetConnection())
            {
                var tableNames = connection.GetTableNames();

                if (tableNames.Contains(_tableName))
                {
                    return;
                }

                _log.Info("Table {tableName} does not exist - it will be created now", _tableName.QualifiedName);

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{_tableName.Schema}')
	EXEC('CREATE SCHEMA {_tableName.Schema}')

----

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{_tableName.Schema}' AND TABLE_NAME = '{_tableName.Name}')
    CREATE TABLE {_tableName.QualifiedName} (
	    [topic] [nvarchar]({_topicLength}) NOT NULL,
	    [address] [nvarchar]({_addressLength}) NOT NULL,
        CONSTRAINT [PK_{_tableName.Schema}_{_tableName.Name}] PRIMARY KEY CLUSTERED 
        (
	        [topic] ASC,
	        [address] ASC
        )
    )
";
                    command.ExecuteNonQuery();
                }

                await connection.Complete();
            }
        }

        /// <summary>
        /// Gets all destination addresses for the given topic
        /// </summary>
        public async Task<string[]> GetSubscriberAddresses(string topic)
        {
            using (var connection = await _connectionProvider.GetConnection())
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"SELECT [address] FROM {_tableName.QualifiedName} WHERE [topic] = @topic";
                    command.Parameters.Add("topic", SqlDbType.NVarChar, _topicLength).Value = topic;

                    var subscriberAddresses = new List<string>();

                    using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                    {
                        while (await reader.ReadAsync().ConfigureAwait(false))
                        {
                            var address = (string)reader["address"];
                            subscriberAddresses.Add(address);
                        }
                    }

                    return subscriberAddresses.ToArray();
                }
            }
        }

        /// <summary>
        /// Registers the given <paramref name="subscriberAddress"/> as a subscriber of the given <paramref name="topic"/>
        /// </summary>
        public async Task RegisterSubscriber(string topic, string subscriberAddress)
        {
            CheckLengths(topic, subscriberAddress);

            using (var connection = await _connectionProvider.GetConnection())
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"
IF NOT EXISTS (SELECT * FROM {_tableName.QualifiedName} WHERE [topic] = @topic AND [address] = @address)
BEGIN
    INSERT INTO {_tableName.QualifiedName} ([topic], [address]) VALUES (@topic, @address)
END";
                    command.Parameters.Add("topic", SqlDbType.NVarChar, _topicLength).Value = topic;
                    command.Parameters.Add("address", SqlDbType.NVarChar, _addressLength).Value = subscriberAddress;

                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                }

                await connection.Complete();
            }
        }

        void CheckLengths(string topic, string subscriberAddress)
        {
            if (topic.Length > _topicLength)
            {
                throw new ArgumentException(
                    $"Cannot register '{subscriberAddress}' as a subscriber of '{topic}' because the length of the topic is greater than {_topicLength} (which is the current MAX length allowed by the current {_tableName} schema)");
            }

            if (subscriberAddress.Length > _addressLength)
            {
                throw new ArgumentException(
                    $"Cannot register '{subscriberAddress}' as a subscriber of '{topic}' because the length of the subscriber address is greater than {_addressLength} (which is the current MAX length allowed by the current {_tableName} schema)");
            }
        }

        /// <summary>
        /// Unregisters the given <paramref name="subscriberAddress"/> as a subscriber of the given <paramref name="topic"/>
        /// </summary>
        public async Task UnregisterSubscriber(string topic, string subscriberAddress)
        {
            CheckLengths(topic, subscriberAddress);

            using (var connection = await _connectionProvider.GetConnection())
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        $@"
DELETE FROM {_tableName.QualifiedName} WHERE [topic] = @topic AND [address] = @address
";
                    command.Parameters.Add("topic", SqlDbType.NVarChar, _topicLength).Value = topic;
                    command.Parameters.Add("address", SqlDbType.NVarChar, _addressLength).Value = subscriberAddress;

                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                }

                await connection.Complete();
            }
        }

        /// <summary>
        /// Gets whether this subscription storage is centralized
        /// </summary>
        public bool IsCentralized { get; }
    }
}
