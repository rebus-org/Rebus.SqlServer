using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Exceptions;
using Rebus.Extensions;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Serialization;
using Rebus.Threading;
using Rebus.Time;
using Rebus.Transport;

namespace Rebus.SqlServer.Transport
{
    /// <summary>
    /// Implementation of <see cref="ITransport"/> that uses SQL Server to do its thing
    /// </summary>
    public class SqlServerTransport : ITransport, IInitializable, IDisposable
    {
        static readonly HeaderSerializer HeaderSerializer = new HeaderSerializer();

        /// <summary>
        /// When a message is sent to this address, it will be deferred into the future!
        /// </summary>
        public const string MagicExternalTimeoutManagerAddress = "##### MagicExternalTimeoutManagerAddress #####";

        /// <summary>
        /// Special message priority header that can be used with the <see cref="SqlServerTransport"/>. The value must be an <see cref="Int32"/>
        /// </summary>
        public const string MessagePriorityHeaderKey = "rbs2-msg-priority";

        /// <summary>
        /// Key of the transport's currently used database connection. Can be retrieved from the context and used e.g.
        /// in a connection provider which is then in turn used in repositories and such. This way, "exactly once delivery" can actually be had.
        /// </summary>
        public const string CurrentConnectionKey = "sql-server-transport-current-connection";

        /// <summary>
        /// Default delay between executing the background cleanup task
        /// </summary>
        public static readonly TimeSpan DefaultExpiredMessagesCleanupInterval = TimeSpan.FromSeconds(20);

        // TODO: This value should probably be used in creating of database
        /// <summary>
        /// Size, in the database, of the recipient column
        /// </summary>
        protected const int RecipientColumnSize = 200;

        /// <summary>
        /// Connection provider for obtaining a database connection
        /// </summary>
        protected readonly IDbConnectionProvider ConnectionProvider;

        private readonly IRebusTime _rebusTime;

        /// <summary>
        /// Name of the table this transport is using for storage
        /// </summary>
        protected readonly TableName ReceiveTableName;

        /// <summary>
        /// Logger
        /// </summary>
        protected readonly ILog Log;
        
        readonly AsyncBottleneck _bottleneck = new AsyncBottleneck(20);
        readonly IAsyncTask _expiredMessagesCleanupTask;
        readonly bool _nativeTimeoutManagerDisabled;
        readonly bool _autoDeleteQueue;
        bool _disposed;

        /// <summary>
        /// Constructs the transport with the given <see cref="IDbConnectionProvider"/>
        /// </summary>
        public SqlServerTransport(IDbConnectionProvider connectionProvider, string inputQueueName, IRebusLoggerFactory rebusLoggerFactory, IAsyncTaskFactory asyncTaskFactory, IRebusTime rebusTime, SqlServerTransportOptions options)
        {
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));
            if (asyncTaskFactory == null) throw new ArgumentNullException(nameof(asyncTaskFactory));

            _rebusTime = rebusTime ?? throw new ArgumentNullException(nameof(rebusTime));
            ConnectionProvider = connectionProvider ?? throw new ArgumentNullException(nameof(connectionProvider));
            ReceiveTableName = inputQueueName != null ? TableName.Parse(inputQueueName) : null;

            Log = rebusLoggerFactory.GetLogger<SqlServerTransport>();

            var cleanupInterval = options.ExpiredMessagesCleanupInterval ?? DefaultExpiredMessagesCleanupInterval;
            var intervalSeconds = (int)cleanupInterval.TotalSeconds;

            _expiredMessagesCleanupTask = asyncTaskFactory.Create("ExpiredMessagesCleanup", PerformExpiredMessagesCleanupCycle, intervalSeconds: intervalSeconds);
            _autoDeleteQueue = options.AutoDeleteQueue;

            _nativeTimeoutManagerDisabled = options.NativeTimeoutManagerDisabled;
        }

        /// <summary>
        /// Initializes the transport by starting a task that deletes expired messages from the SQL table
        /// </summary>
        public void Initialize()
        {
            if (ReceiveTableName == null) return;

            _expiredMessagesCleanupTask.Start();
        }

        /// <summary>
        /// Gets the name that this SQL transport will use to query by when checking the messages table
        /// </summary>
        public string Address => ReceiveTableName?.QualifiedName;

        /// <summary>
        /// Creates the table named after the given <paramref name="address"/>
        /// </summary>
        public void CreateQueue(string address)
        {
            if (address == null) return;

            var tableName = TableName.Parse(address);

            AsyncHelpers.RunSync(() => EnsureTableIsCreatedAsync(tableName));
        }

        /// <summary>
        /// Checks if the table with the configured name exists - if not, it will be created
        /// </summary>
        public void EnsureTableIsCreated()
        {
            AsyncHelpers.RunSync(() => EnsureTableIsCreatedAsync(ReceiveTableName));
        }

        async Task EnsureTableIsCreatedAsync(TableName tableName)
        {
            try
            {
                await InnerEnsureTableIsCreatedAsync(tableName).ConfigureAwait(false);
            }
            catch (Exception)
            {
                // if it fails the first time, and if it's because of some kind of conflict,
                // we should run it again and see if the situation has stabilized
                await InnerEnsureTableIsCreatedAsync(tableName).ConfigureAwait(false);
            }
        }

        async Task InnerEnsureTableIsCreatedAsync(TableName tableName)
        {
            using (var connection = await ConnectionProvider.GetConnection())
            {
                var tableNames = connection.GetTableNames();

                if (tableNames.Contains(tableName))
                {
                    Log.Info("Database already contains a table named {tableName} - will not create anything", tableName.QualifiedName);
                    await connection.Complete();
                    return;
                }

                Log.Info("Table {tableName} does not exist - it will be created now", tableName.QualifiedName);

                var receiveIndexName = $"IDX_RECEIVE_{tableName.Schema}_{tableName.Name}";
                var expirationIndexName = $"IDX_EXPIRATION_{tableName.Schema}_{tableName.Name}";

                ExecuteCommands(connection, $@"
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{tableName.Schema}')
	EXEC('CREATE SCHEMA [{tableName.Schema}]')

----

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{tableName.Schema}' AND TABLE_NAME = '{tableName.Name}')
    CREATE TABLE {tableName.QualifiedName}
    (
	    [id] [bigint] IDENTITY(1,1) NOT NULL,
	    [priority] [int] NOT NULL,
        [expiration] [datetimeoffset] NOT NULL,
        [visible] [datetimeoffset] NOT NULL,
	    [headers] [varbinary](max) NOT NULL,
	    [body] [varbinary](max) NOT NULL,
        CONSTRAINT [PK_{tableName.Schema}_{tableName.Name}] PRIMARY KEY CLUSTERED 
        (
	        [priority] ASC,
	        [id] ASC
        )
    )

----

-- Drop the V0 Receive Index
-- v0 index was (Priority, Visible, Expiration, Id)
-- We can find this by looking for the index with priority as is_descending_key = 0
IF EXISTS (SELECT 1 FROM sys.indexes I JOIN sys.index_columns IC ON I.object_id = OBJECT_ID('{tableName.QualifiedName}') AND I.name = '{receiveIndexName}' AND IC.object_id = I.object_id AND IC.index_id = I.index_id JOIN sys.columns C ON C.object_id = IC.object_id AND C.column_id = IC.column_id AND C.name = 'priority' and IC.is_descending_key = 0)
BEGIN
    DROP INDEX [{receiveIndexName}] ON {tableName.QualifiedName}
END

----

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = '{receiveIndexName}')
    CREATE NONCLUSTERED INDEX [{receiveIndexName}] ON {tableName.QualifiedName}
    (
        [priority] DESC,
        [visible] ASC,
        [id] ASC,
        [expiration] ASC
    )

----

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = '{expirationIndexName}')
    CREATE NONCLUSTERED INDEX [{expirationIndexName}] ON {tableName.QualifiedName}
    (
        [expiration] ASC
    )

");

                var additional = AdditionalSchemaModifications(tableName);
                ExecuteCommands(connection, additional);
                await connection.Complete();
            }
        }

        /// <summary>
        /// Provides an opportunity for derived implementations to also update the schema
        /// </summary>
        /// <param name="tableName">Name of the table to create schema modifications for</param>
        protected virtual string AdditionalSchemaModifications(TableName tableName)
        {
            return string.Empty;
        }
        
        /// <summary>
        /// Checks if the table with the configured name exists - if it is, it will be dropped
        /// </summary>
        void EnsureTableIsDropped()
        {
            try
            {
                AsyncHelpers.RunSync(() => EnsureTableIsDroppedAsync(ReceiveTableName));
            }
            catch
            {
                // if it failed because of a collision between another thread doing the same thing, just try again once:
                AsyncHelpers.RunSync(() => EnsureTableIsDroppedAsync(ReceiveTableName));
            }
        }        

        async Task EnsureTableIsDroppedAsync(TableName tableName)
        {
            try
            {
                await InnerEnsureTableIsDroppedAsync(tableName).ConfigureAwait(false);
            }
            catch (Exception)
            {
                // if it fails the first time, and if it's because of some kind of conflict,
                // we should run it again and see if the situation has stabilized
                await InnerEnsureTableIsDroppedAsync(tableName).ConfigureAwait(false);
            }
        }

        async Task InnerEnsureTableIsDroppedAsync(TableName tableName)
        {
            using (var connection = await ConnectionProvider.GetConnection())
            {
                var tableNames = connection.GetTableNames();

                if (!tableNames.Contains(tableName))
                {
                    Log.Info("A table named {tableName} doesn't exist", tableName.QualifiedName);
                    await connection.Complete();
                    return;
                }

                Log.Info("Table {tableName} exists - it will be dropped now", tableName.QualifiedName);

                ExecuteCommands(connection, $@"
IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{tableName.Schema}' AND TABLE_NAME = '{tableName.Name}')
    DROP TABLE {tableName.QualifiedName};");

                var additional = AdditionalSchemaModificationsOnDeleteQueue();
                ExecuteCommands(connection, additional);
                await connection.Complete();
            }
        }
        
        /// <summary>
        /// Provides an oppurtunity for derived implementations to also update the schema when the queue is deleted automatically 
        /// </summary>
        protected virtual string AdditionalSchemaModificationsOnDeleteQueue()
        {
            return string.Empty;
        }        

        static void ExecuteCommands(IDbConnection connection, string sqlCommands)
        {
            foreach (var sqlCommand in sqlCommands.Split(new[] { "----" }, StringSplitOptions.RemoveEmptyEntries))
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = sqlCommand;

                    Execute(command);
                }
            }
        }

        static void Execute(IDbCommand command)
        {
            try
            {
                command.ExecuteNonQuery();
            }
            catch (SqlException exception)
            {
                throw new RebusApplicationException(exception, $@"Error executing SQL command
{command.CommandText}
");
            }
        }

        /// <summary>
        /// Sends the given transport message to the specified destination queue address by adding it to the queue's table.
        /// </summary>
        public virtual async Task Send(string destinationAddress, TransportMessage message, ITransactionContext context)
        {
            var connection = await GetConnection(context).ConfigureAwait(false);

            var destinationAddressToUse = GetDestinationAddressToUse(destinationAddress, message);

            try
            {
                await InnerSend(destinationAddressToUse, message, connection).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                throw new RebusApplicationException(e, $"Unable to send to destination {destinationAddress}");
            }
        }

        /// <summary>
        /// Receives the next message by querying the input queue table for a message with a recipient matching this transport's <see cref="Address"/>
        /// </summary>
        public async Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
            using (await _bottleneck.Enter(cancellationToken).ConfigureAwait(false))
            {
                return await ReceiveInternal(context, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Handle retrieving a message from the queue, decoding it, and performing any transaction maintenance.
        /// </summary>
        /// <param name="context">Tranasction context the receive is operating on</param>
        /// <param name="cancellationToken">Token to abort processing</param>
        /// <returns>A <seealso cref="TransportMessage"/> or <c>null</c> if no message can be dequeued</returns>
        protected virtual async Task<TransportMessage> ReceiveInternal(ITransactionContext context, CancellationToken cancellationToken)
        {
            var connection = await GetConnection(context).ConfigureAwait(false);

            TransportMessage receivedTransportMessage;

            using (var selectCommand = connection.CreateCommand())
            {
                selectCommand.CommandText = $@"

	SET NOCOUNT ON

	;WITH TopCTE AS (
		SELECT	TOP 1
				[id],
				[headers],
				[body]
		FROM	{ReceiveTableName.QualifiedName} M WITH (ROWLOCK, READPAST, READCOMMITTEDLOCK)
		WHERE	
                M.[visible] < sysdatetimeoffset()
		AND		M.[expiration] > sysdatetimeoffset()
		ORDER
		BY		[priority] DESC,
				[visible] ASC,
				[id] ASC
	)
	DELETE	FROM TopCTE
	OUTPUT	deleted.[id] as [id],
			deleted.[headers] as [headers],
			deleted.[body] as [body]
					
	SET NOCOUNT OFF
						";

                try
                {
                    using (var reader = await selectCommand.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
                    {
                        receivedTransportMessage = await ExtractTransportMessageFromReader(reader, cancellationToken).ConfigureAwait(false);
                    }
                }
                catch (Exception exception) when (cancellationToken.IsCancellationRequested)
                {
                    // ADO.NET does not throw the right exception when the task gets cancelled - therefore we need to do this:
                    throw new TaskCanceledException("Receive operation was cancelled", exception);
                }
            }

            return receivedTransportMessage;
        }

        /// <summary>
        /// Maps a <seealso cref="SqlDataReader"/> that's read a result from the message table into a <seealso cref="TransportMessage"/>
        /// </summary>
        /// <returns>A <seealso cref="TransportMessage"/> representing the row or <c>null</c> if no row was available</returns>
        protected static async Task<TransportMessage> ExtractTransportMessageFromReader(SqlDataReader reader, CancellationToken cancellationToken)
        {
            if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false) == false)
            {
                return null;
            }

            var headers = reader["headers"];
            var headersDictionary = HeaderSerializer.Deserialize((byte[])headers);
            var body = (byte[])reader["body"];

            return new TransportMessage(headersDictionary, body);
        }


        /// <summary>
        /// Gets the address a message will actually be sent to. Handles deferred messsages.
        /// </summary>
        protected static string GetDestinationAddressToUse(string destinationAddress, TransportMessage message)
        {
            return string.Equals(destinationAddress, MagicExternalTimeoutManagerAddress, StringComparison.OrdinalIgnoreCase)
                ? GetDeferredRecipient(message)
                : destinationAddress;
        }

        static string GetDeferredRecipient(TransportMessage message)
        {
            if (message.Headers.TryGetValue(Headers.DeferredRecipient, out var destination))
            {
                return destination;
            }

            throw new InvalidOperationException($"Attempted to defer message, but no '{Headers.DeferredRecipient}' header was on the message");
        }

        /// <summary>
        /// Performs persistence of a message to the underlying table
        /// </summary>
        /// <param name="destinationAddress">Address the message will be sent to</param>
        /// <param name="message">Message to be sent</param>
        /// <param name="connection">Connection to use for writing to the database</param>
        protected async Task InnerSend(string destinationAddress, TransportMessage message, IDbConnection connection)
        {
            var sendTable = TableName.Parse(destinationAddress);
            
            using (var command = connection.CreateCommand())
            {
                command.CommandText = $@"
INSERT INTO {sendTable.QualifiedName}
(
    [headers],
    [body],
    [priority],
    [visible],
    [expiration]
)
VALUES
(
    @headers,
    @body,
    @priority,
    dateadd(ms, @visiblemilliseconds, dateadd(ss, @visibletotalseconds, sysdatetimeoffset())),
    dateadd(ms, @ttlmilliseconds, dateadd(ss, @ttltotalseconds, sysdatetimeoffset()))
)";

                var headers = message.Headers.Clone();

                var priority = GetMessagePriority(headers);
                var visible = GetInitialVisibilityDelay(headers);
                var ttl = GetTtl(headers);

                // must be last because the other functions on the headers might change them
                var serializedHeaders = HeaderSerializer.Serialize(headers);

                command.Parameters.Add("headers", SqlDbType.VarBinary, MathUtil.GetNextPowerOfTwo(serializedHeaders.Length)).Value = serializedHeaders;
                command.Parameters.Add("body", SqlDbType.VarBinary, MathUtil.GetNextPowerOfTwo(message.Body.Length)).Value = message.Body;
                command.Parameters.Add("priority", SqlDbType.Int).Value = priority;
                command.Parameters.Add("visibletotalseconds", SqlDbType.Int).Value = (int)visible.TotalSeconds;
                command.Parameters.Add("visiblemilliseconds", SqlDbType.Int).Value = visible.Milliseconds;
                command.Parameters.Add("ttltotalseconds", SqlDbType.Int).Value = (int)ttl.TotalSeconds;
                command.Parameters.Add("ttlmilliseconds", SqlDbType.Int).Value = ttl.Milliseconds;

                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        TimeSpan GetInitialVisibilityDelay(IDictionary<string, string> headers)
        {
            if (_nativeTimeoutManagerDisabled)
            {
                return TimeSpan.Zero;
            }

            if (!headers.TryGetValue(Headers.DeferredUntil, out var deferredUntilDateTimeOffsetString))
            {
                return TimeSpan.Zero;
            }

            var deferredUntilTime = deferredUntilDateTimeOffsetString.ToDateTimeOffset();

            headers.Remove(Headers.DeferredUntil);

            var visibilityDelay = deferredUntilTime - _rebusTime.Now;

            return visibilityDelay;
        }

        static TimeSpan GetTtl(IReadOnlyDictionary<string, string> headers)
        {
            const int defaultTtlSecondsAbout60Years = int.MaxValue;

            if (!headers.ContainsKey(Headers.TimeToBeReceived))
            {
                return TimeSpan.FromSeconds(defaultTtlSecondsAbout60Years);
            }

            var timeToBeReceivedStr = headers[Headers.TimeToBeReceived];
            var timeToBeReceived = TimeSpan.Parse(timeToBeReceivedStr);

            return timeToBeReceived;
        }

        async Task PerformExpiredMessagesCleanupCycle()
        {
            var results = 0;
            var stopwatch = Stopwatch.StartNew();

            while (true)
            {
                using (var connection = await ConnectionProvider.GetConnection())
                {
                    int affectedRows;

                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText =
                            $@"
;with TopCTE as (
	SELECT TOP 1 [id] FROM {ReceiveTableName.QualifiedName} WITH (ROWLOCK, READPAST)
				WHERE 
                    [expiration] < sysdatetimeoffset()
)
DELETE FROM TopCTE
";

                        affectedRows = await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                    }

                    results += affectedRows;

                    await connection.Complete();

                    if (affectedRows == 0) break;
                }
            }

            if (results > 0)
            {
                Log.Info("Performed expired messages cleanup in {cleanupTimeSeconds} - {expiredMessageCount} expired messages with recipient {queueName} were deleted",
                    stopwatch.Elapsed.TotalSeconds, results, ReceiveTableName.QualifiedName);
            }
        }

        static int GetMessagePriority(Dictionary<string, string> headers)
        {
            var valueOrNull = headers.GetValueOrNull(MessagePriorityHeaderKey);
            if (valueOrNull == null) return 0;

            try
            {
                return int.Parse(valueOrNull);
            }
            catch (Exception exception)
            {
                throw new FormatException($"Could not parse '{valueOrNull}' into an Int32!", exception);
            }
        }

        Task<IDbConnection> GetConnection(ITransactionContext context)
        {
            return context
                .GetOrAdd(CurrentConnectionKey,
                    async () =>
                    {
                        var dbConnection = await ConnectionProvider.GetConnection();
                        context.OnCommitted(async ctx => await dbConnection.Complete());
                        context.OnDisposed(ctx =>
                        {
                            dbConnection.Dispose();
                        });
                        return dbConnection;
                    });
        }

        /// <summary>
        /// Shuts down the background timer
        /// </summary>
        public void Dispose()
        {
            if (_disposed) return;

            try
            {
                _expiredMessagesCleanupTask.Dispose();
                if (_autoDeleteQueue)
                    EnsureTableIsDropped();
            }
            finally
            {
                _disposed = true;
            }
        }
    }
}
