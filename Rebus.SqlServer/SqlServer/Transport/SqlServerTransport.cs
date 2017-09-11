using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Rebus.Bus;
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
        /// Default interval that will be used for <see cref="ExpiredMessagesCleanupInterval"/> unless it is explicitly set to something else
        /// </summary>
        public static readonly TimeSpan DefaultExpiredMessagesCleanupInterval = TimeSpan.FromSeconds(20);

        /// <summary>
        /// Size, in the database, of the recipient column
        /// </summary>
        protected const int RecipientColumnSize = 200;

        /// <summary>
        /// Connection provider for obtaining a database connection
        /// </summary>
        protected readonly IDbConnectionProvider ConnectionProvider;

        /// <summary>
        /// Name of the queue being processed by this transport
        /// </summary>
        protected readonly string InputQueueName;

        /// <summary>
        /// Name of the table this transport is using for storage
        /// </summary>
        protected readonly TableName TableName;

        readonly AsyncBottleneck _bottleneck = new AsyncBottleneck(20);


        readonly ILog _log;

        readonly IAsyncTask _expiredMessagesCleanupTask;
        bool _disposed;

        /// <summary>
        /// Constructs the transport with the given <see cref="IDbConnectionProvider"/>, using the specified <paramref name="tableName"/> to send/receive messages,
        /// querying for messages with recipient = <paramref name="inputQueueName"/>
        /// </summary>
        public SqlServerTransport(IDbConnectionProvider connectionProvider, string tableName, string inputQueueName, IRebusLoggerFactory rebusLoggerFactory, IAsyncTaskFactory asyncTaskFactory)
        {
            if (connectionProvider == null) throw new ArgumentNullException(nameof(connectionProvider));
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));
            if (asyncTaskFactory == null) throw new ArgumentNullException(nameof(asyncTaskFactory));

            ConnectionProvider = connectionProvider;
            TableName = TableName.Parse(tableName);
            InputQueueName = inputQueueName;
            _log = rebusLoggerFactory.GetLogger<SqlServerTransport>();

            ExpiredMessagesCleanupInterval = DefaultExpiredMessagesCleanupInterval;

            _expiredMessagesCleanupTask = asyncTaskFactory.Create("ExpiredMessagesCleanup", PerformExpiredMessagesCleanupCycle, intervalSeconds: 60);
        }

        /// <summary>
        /// Initializes the transport by starting a task that deletes expired messages from the SQL table
        /// </summary>
        public void Initialize()
        {
            if (InputQueueName == null) return;

            _expiredMessagesCleanupTask.Start();
        }

        /// <summary>
        /// Configures the interval between periodic deletion of expired messages. Defaults to <see cref="DefaultExpiredMessagesCleanupInterval"/>
        /// </summary>
        public TimeSpan ExpiredMessagesCleanupInterval { get; set; }

        /// <summary>
        /// Gets the name that this SQL transport will use to query by when checking the messages table
        /// </summary>
        public string Address => InputQueueName;

        /// <summary>
        /// The SQL transport doesn't really have queues, so this function does nothing
        /// </summary>
        public void CreateQueue(string address)
        {
        }

        /// <summary>
        /// Checks if the table with the configured name exists - if not, it will be created
        /// </summary>
        public void EnsureTableIsCreated()
        {
            AsyncHelpers.RunSync(EnsureTableIsCreatedAsync);
        }

        /// <summary>
        /// Provides an oppurtunity for derived implementations to also update the schema
        /// </summary>
        protected virtual string AdditionalSchemaModifications(IDbConnection connection)
        {
            return string.Empty;
        }

        async Task EnsureTableIsCreatedAsync()
        {
            using (var connection = await ConnectionProvider.GetConnection())
            {
                var tableNames = connection.GetTableNames();
                string additional = null;

                if (tableNames.Contains(TableName))
                {
                    _log.Info("Database already contains a table named {tableName} - will not create anything", TableName.QualifiedName);
                    additional = AdditionalSchemaModifications(connection);
                    ExecuteCommands(connection, additional);

                    await connection.Complete();
                    return;
                }

                _log.Info("Table {tableName} does not exist - it will be created now", TableName.QualifiedName);

                var receiveIndexName = $"IDX_RECEIVE_{TableName.Schema}_{TableName.Name}";
                var expirationIndexName = $"IDX_EXPIRATION_{TableName.Schema}_{TableName.Name}";

                ExecuteCommands(connection, $@"
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{TableName.Schema}')
	EXEC('CREATE SCHEMA [{TableName.Schema}]')

----

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{TableName.Schema}' AND TABLE_NAME = '{TableName.Name}')
    CREATE TABLE {TableName.QualifiedName}
    (
	    [id] [bigint] IDENTITY(1,1) NOT NULL,
	    [recipient] [nvarchar](200) NOT NULL,
	    [priority] [int] NOT NULL,
        [expiration] [datetime2] NOT NULL,
        [visible] [datetime2] NOT NULL,
	    [headers] [varbinary](max) NOT NULL,
	    [body] [varbinary](max) NOT NULL,
        CONSTRAINT [PK_{TableName.Schema}_{TableName.Name}] PRIMARY KEY CLUSTERED 
        (
	        [recipient] ASC,
	        [priority] ASC,
	        [id] ASC
        )
    )

----

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = '{receiveIndexName}')
    CREATE NONCLUSTERED INDEX [{receiveIndexName}] ON {TableName.QualifiedName}
    (
	    [recipient] ASC,
	    [priority] ASC,
        [visible] ASC,
        [expiration] ASC,
	    [id] ASC
    )

----

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = '{expirationIndexName}')
    CREATE NONCLUSTERED INDEX [{expirationIndexName}] ON {TableName.QualifiedName}
    (
        [expiration] ASC
    )

");

                additional = AdditionalSchemaModifications(connection);
                ExecuteCommands(connection, additional);

                await connection.Complete();
            }
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
        /// Sends the given transport message to the specified logical destination address by adding it to the messages table.
        /// </summary>
        public virtual async Task Send(string destinationAddress, TransportMessage message, ITransactionContext context)
        {
            var connection = await GetConnection(context);

            var destinationAddressToUse = GetDestinationAddressToUse(destinationAddress, message);

            await InnerSend(destinationAddressToUse, message, connection);
        }

        /// <summary>
        /// Receives the next message by querying the messages table for a message with a recipient matching this transport's <see cref="Address"/>
        /// </summary>
        public async Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
            using (await _bottleneck.Enter(cancellationToken))
            {
                return await ReceiveInternal(context, cancellationToken);
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
            var connection = await GetConnection(context);

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
		FROM	{TableName.QualifiedName} M WITH (ROWLOCK, READPAST)
		WHERE	M.[recipient] = @recipient
		AND		M.[visible] < getdate()
		AND		M.[expiration] > getdate()
		ORDER
		BY		[priority] ASC,
				[id] ASC
	)
	DELETE	FROM TopCTE
	OUTPUT	deleted.[id] as [id],
			deleted.[headers] as [headers],
			deleted.[body] as [body]
						
						";

                selectCommand.Parameters.Add("recipient", SqlDbType.NVarChar, RecipientColumnSize).Value = InputQueueName;

                try
                {
                    using (var reader = await selectCommand.ExecuteReaderAsync(cancellationToken))
                    {
                        receivedTransportMessage = await ExtractTransportMessageFromReader(reader, cancellationToken);
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
            if (await reader.ReadAsync(cancellationToken) == false)
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
            return string.Equals(destinationAddress, MagicExternalTimeoutManagerAddress, StringComparison.CurrentCultureIgnoreCase)
                ? GetDeferredRecipient(message)
                : destinationAddress;
        }

        static string GetDeferredRecipient(TransportMessage message)
        {
            string destination;

            if (message.Headers.TryGetValue(Headers.DeferredRecipient, out destination))
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
            using (var command = connection.CreateCommand())
            {
                command.CommandText = $@"
INSERT INTO {TableName.QualifiedName}
(
    [recipient],
    [headers],
    [body],
    [priority],
    [visible],
    [expiration]
)
VALUES
(
    @recipient,
    @headers,
    @body,
    @priority,
    dateadd(ss, @visible, getdate()),
    dateadd(ss, @ttlseconds, getdate())
)";

                var headers = message.Headers.Clone();

                var priority = GetMessagePriority(headers);
                var initialVisibilityDelay = GetInitialVisibilityDelay(headers);
                var ttlSeconds = GetTtlSeconds(headers);

                // must be last because the other functions on the headers might change them
                var serializedHeaders = HeaderSerializer.Serialize(headers);

                command.Parameters.Add("recipient", SqlDbType.NVarChar, RecipientColumnSize).Value = destinationAddress;
                command.Parameters.Add("headers", SqlDbType.VarBinary).Value = serializedHeaders;
                command.Parameters.Add("body", SqlDbType.VarBinary).Value = message.Body;
                command.Parameters.Add("priority", SqlDbType.Int).Value = priority;
                command.Parameters.Add("ttlseconds", SqlDbType.Int).Value = ttlSeconds;
                command.Parameters.Add("visible", SqlDbType.Int).Value = initialVisibilityDelay;

                await command.ExecuteNonQueryAsync();
            }
        }

        static int GetInitialVisibilityDelay(IDictionary<string, string> headers)
        {
            string deferredUntilDateTimeOffsetString;

            if (!headers.TryGetValue(Headers.DeferredUntil, out deferredUntilDateTimeOffsetString))
            {
                return 0;
            }

            var deferredUntilTime = deferredUntilDateTimeOffsetString.ToDateTimeOffset();

            headers.Remove(Headers.DeferredUntil);

            return (int)(deferredUntilTime - RebusTime.Now).TotalSeconds;
        }

        static int GetTtlSeconds(IReadOnlyDictionary<string, string> headers)
        {
            const int defaultTtlSecondsAbout60Years = int.MaxValue;

            if (!headers.ContainsKey(Headers.TimeToBeReceived))
                return defaultTtlSecondsAbout60Years;

            var timeToBeReceivedStr = headers[Headers.TimeToBeReceived];
            var timeToBeReceived = TimeSpan.Parse(timeToBeReceivedStr);

            return (int)timeToBeReceived.TotalSeconds;
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
	SELECT TOP 1 [id] FROM {TableName.QualifiedName} WITH (ROWLOCK, READPAST)
				WHERE [recipient] = @recipient 
					AND [expiration] < getdate()
)
DELETE FROM TopCTE
";
                        command.Parameters.Add("recipient", SqlDbType.NVarChar, RecipientColumnSize).Value = InputQueueName;

                        affectedRows = await command.ExecuteNonQueryAsync();
                    }

                    results += affectedRows;

                    await connection.Complete();

                    if (affectedRows == 0) break;
                }
            }

            if (results > 0)
            {
                _log.Info("Performed expired messages cleanup in {cleanupTimeSeconds} - {expiredMessageCount} expired messages with recipient {queueName} were deleted",
                    stopwatch.Elapsed.TotalSeconds, results, InputQueueName);
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
                        context.OnCommitted(async () => await dbConnection.Complete());
                        context.OnDisposed(() =>
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
            }
            finally
            {
                _disposed = true;
            }
        }
    }
}