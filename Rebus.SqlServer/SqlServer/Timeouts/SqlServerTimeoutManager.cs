using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Rebus.Logging;
using Rebus.Serialization;
using Rebus.Time;
using Rebus.Timeouts;
// ReSharper disable AccessToDisposedClosure

namespace Rebus.SqlServer.Timeouts
{
    /// <summary>
    /// Implementation of <see cref="ITimeoutManager"/> that uses SQL Server to store messages until it's time to deliver them.
    /// </summary>
    public class SqlServerTimeoutManager : ITimeoutManager
    {
        static readonly HeaderSerializer HeaderSerializer = new HeaderSerializer();
        readonly IDbConnectionProvider _connectionProvider;
        private readonly IRebusTime _rebusTime;
        readonly TableName _tableName;
        readonly ILog _log;

        /// <summary>
        /// Constructs the timeout manager, using the specified connection provider and table to store the messages until they're due.
        /// </summary>
        public SqlServerTimeoutManager(IDbConnectionProvider connectionProvider, string tableName, IRebusLoggerFactory rebusLoggerFactory, IRebusTime rebusTime)
        {
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));

            _connectionProvider = connectionProvider ?? throw new ArgumentNullException(nameof(connectionProvider));
            _rebusTime = rebusTime ?? throw new ArgumentNullException(nameof(rebusTime));

            _tableName = TableName.Parse(tableName);
            _log = rebusLoggerFactory.GetLogger<SqlServerTimeoutManager>();
        }

        /// <summary>
        /// Creates the due messages table if necessary
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

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{_tableName.Schema}' AND TABLE_NAME = '{
                            _tableName.Name
                        }')
    CREATE TABLE {_tableName.QualifiedName} (
        [id] [bigint] IDENTITY(1,1) NOT NULL,
	    [due_time] [datetimeoffset](7) NOT NULL,
	    [headers] [nvarchar](MAX) NOT NULL,
	    [body] [varbinary](MAX) NOT NULL,
        CONSTRAINT [PK_{_tableName.Schema}_{_tableName.Name}] PRIMARY KEY NONCLUSTERED 
        (
	        [id] ASC
        )
    )
";
                    command.ExecuteNonQuery();
                }

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_{_tableName.Schema}_{_tableName.Name}_DueTime')
    CREATE CLUSTERED INDEX [IX_{_tableName.Schema}_{_tableName.Name}_DueTime] ON {_tableName.QualifiedName}
    (
	    [due_time] ASC
    )";

                    command.ExecuteNonQuery();
                }

                await connection.Complete();
            }
        }

        /// <summary>
        /// Defers the message to the time specified by <paramref name="approximateDueTime"/> at which point in time the message will be
        /// returned to whoever calls <see cref="GetDueMessages"/>
        /// </summary>
        public async Task Defer(DateTimeOffset approximateDueTime, Dictionary<string, string> headers, byte[] body)
        {
            var headersString = HeaderSerializer.SerializeToString(headers);

            using (var connection = await _connectionProvider.GetConnection())
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"INSERT INTO {_tableName.QualifiedName} ([due_time], [headers], [body]) VALUES (@due_time, @headers, @body)";

                    command.Parameters.Add("due_time", SqlDbType.DateTimeOffset, 7).Value = approximateDueTime;
                    command.Parameters.Add("headers", SqlDbType.NVarChar, MathUtil.GetNextPowerOfTwo(headersString.Length)).Value = headersString;
                    command.Parameters.Add("body", SqlDbType.VarBinary, MathUtil.GetNextPowerOfTwo(body.Length)).Value = body;

                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                }

                await connection.Complete();
            }
        }

        /// <summary>
        /// Gets messages due for delivery at the current time
        /// </summary>
        public async Task<DueMessagesResult> GetDueMessages()
        {
            var connection = await _connectionProvider.GetConnection();
            try
            {
                var dueMessages = new List<DueMessage>();

                const int maxDueTimeouts = 1000;

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        $@"
SELECT 
    [id],
    [headers],
    [body]
FROM {_tableName.QualifiedName} WITH (UPDLOCK, READPAST, ROWLOCK)
WHERE [due_time] <= @current_time 
ORDER BY [due_time] ASC
";

                    command.Parameters.Add("current_time", SqlDbType.DateTimeOffset, 7).Value = _rebusTime.Now;

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            var id = Convert.ToInt64(reader["id"]);
                            var headersString = (string)reader["headers"];
                            var headers = HeaderSerializer.DeserializeFromString(headersString);
                            var body = (byte[])reader["body"];

                            var sqlTimeout = new DueMessage(headers, body, async () =>
                            {
                                using (var deleteCommand = connection.CreateCommand())
                                {
                                    deleteCommand.CommandText = $"DELETE FROM {_tableName.QualifiedName} WHERE [id] = @id";
                                    deleteCommand.Parameters.Add("id", SqlDbType.BigInt).Value = id;
                                    await deleteCommand.ExecuteNonQueryAsync().ConfigureAwait(false);
                                }
                            });

                            dueMessages.Add(sqlTimeout);

                            if (dueMessages.Count >= maxDueTimeouts) break;
                        }
                    }

                    return new DueMessagesResult(dueMessages, async () =>
                    {
                        using (connection)
                        {
                            await connection.Complete();
                        }
                    });
                }
            }
            catch (Exception)
            {
                connection.Dispose();
                throw;
            }
        }
    }
}
