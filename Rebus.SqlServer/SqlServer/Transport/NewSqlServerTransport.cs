using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Rebus.Bus;
using Rebus.Exceptions;
using Rebus.Extensions;
using Rebus.Messages;
using Rebus.Serialization;
using Rebus.Time;
using Rebus.Transport;

namespace Rebus.SqlServer.Transport
{
    class NewSqlServerTransport : AbstractRebusTransport, IInitializable
    {
        /// <summary>
        /// Special message priority header that can be used with the <see cref="NewSqlServerTransport"/>. The value must be an <see cref="Int32"/>
        /// </summary>
        public const string MessagePriorityHeaderKey = "rbs2-msg-priority";

        static readonly HeaderSerializer HeaderSerializer = new HeaderSerializer();

        readonly IDbConnectionProvider _connectionProvider;
        readonly string _inputQueueName;
        readonly IRebusTime _rebusTime;
        readonly string _schema;
        readonly bool _isClient;

        public NewSqlServerTransport(IDbConnectionProvider connectionProvider, IRebusTime rebusTime, string inputQueueName, string schema) : base(inputQueueName)
        {
            _connectionProvider = connectionProvider ?? throw new ArgumentNullException(nameof(connectionProvider));
            _rebusTime = rebusTime ?? throw new ArgumentNullException(nameof(rebusTime));
            _schema = schema ?? throw new ArgumentNullException(nameof(schema));
            _inputQueueName = inputQueueName;
            _isClient = string.IsNullOrWhiteSpace(inputQueueName);
        }

        public override void CreateQueue(string address)
        {
            AsyncHelpers.RunSync(() => EnsureTableExists(_schema, address));
        }

        public void Initialize()
        {
            if (_isClient) return;

            CreateQueue(_inputQueueName);
        }

        async Task EnsureTableExists(string schema, string tableName)
        {
            var attempts = 5;

            while (true)
            {
                try
                {
                    using (var connection = await _connectionProvider.GetConnection())
                    {
                        if (connection.GetTableNames()
                            .Any(t => t.Schema == schema && t.Name == tableName)) return;

                        var sql = $@"

create table [{schema}].[{tableName}](
	[id] [bigint] IDENTITY(1,1) NOT NULL,
	[priority] [int] NOT NULL,
	[expiration] [datetimeoffset](7) NOT NULL,
	[visible] [datetimeoffset](7) NOT NULL,
	[headers] [varbinary](max) NOT NULL,
	[body] [varbinary](max) NOT NULL,
	[leaseduntil] [datetimeoffset](7) NULL,
	[leasedby] [nvarchar](200) NULL,
	[leasedat] [datetimeoffset](7) NULL,
    constraint [PK_{schema}_{tableName}] primary key clustered
    (
	    [priority] ASC,
	    [id] ASC
    )
)

";


                        await ExecuteNonQuery(connection, sql);

                        await connection.Complete();
                    }

                    return;
                }
                catch
                {
                    attempts--;
                    if (attempts > 0) continue;
                    throw;
                }
            }
        }

        static async Task ExecuteNonQuery(IDbConnection connection, string sql)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = sql;

                try
                {
                    await command.ExecuteNonQueryAsync();
                }
                catch (Exception exception)
                {
                    throw new RebusApplicationException(exception, $@"Error when executing the following SQL:

{sql}");
                }
            }
        }

        protected override async Task SendOutgoingMessages(IEnumerable<OutgoingMessage> outgoingMessages, ITransactionContext context)
        {
            var messagesByDestination = outgoingMessages.GroupBy(m => m.DestinationAddress).ToList();

            using (var connection = await _connectionProvider.GetConnection())
            {
                foreach (var messages in messagesByDestination)
                {
                    var schema = _schema;
                    var tableName = messages.Key;
                    var transportMessages = messages.Select(o => o.TransportMessage).ToArray();

                    await InsertRows(connection, transportMessages, schema, tableName);
                }

                await connection.Complete();
            }
        }

        async Task InsertRows(IDbConnection connection, TransportMessage[] messages, string schema, string tableName)
        {
            IEnumerable<string> GetValues()
            {
                return messages
                    .Select((m, idx) => $@"
(
    @headers{idx},
    @body{idx},
    @priority{idx},
    dateadd(ss, @visible{idx}, getdate()),
    dateadd(ss, @ttlseconds{idx}, getdate())
)
");
            }

            var sql = $@"

INSERT INTO [{schema}].[{tableName}]
(
    [headers],
    [body],
    [priority],
    [visible],
    [expiration]
)
VALUES
{string.Join($",{Environment.NewLine}", GetValues())}

";
            using (var command = connection.CreateCommand())
            {
                command.CommandText = sql;

                for (var idx = 0; idx < messages.Length; idx++)
                {
                    var message = messages[idx];
                    var headers = message.Headers;
                    var serializedHeaders = HeaderSerializer.Serialize(headers);
                    var priority = GetMessagePriority(headers);
                    var initialVisibilityDelay = GetInitialVisibilityDelay(headers);
                    var ttlSeconds = GetTtlSeconds(headers);

                    command.Parameters.Add($"headers{idx}", SqlDbType.VarBinary, MathUtil.GetNextPowerOfTwo(serializedHeaders.Length)).Value = serializedHeaders;
                    command.Parameters.Add($"body{idx}", SqlDbType.VarBinary, MathUtil.GetNextPowerOfTwo(message.Body.Length)).Value = message.Body;
                    command.Parameters.Add($"priority{idx}", SqlDbType.Int).Value = priority;
                    command.Parameters.Add($"ttlseconds{idx}", SqlDbType.Int).Value = ttlSeconds;
                    command.Parameters.Add($"visible{idx}", SqlDbType.Int).Value = initialVisibilityDelay;
                }

                await command.ExecuteNonQueryAsync();
            }
        }

        public override async Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
            TransportMessage transportMessage;

            using (var connection = await _connectionProvider.GetConnection())
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandType = CommandType.Text;
                    command.CommandText = $@"
;WITH TopCTE AS (
	SELECT	TOP 1
			[id],
			[headers],
			[body],
			[leasedat],
			[leaseduntil],
			[leasedby]
	FROM	[{_schema}].[{_inputQueueName}] M WITH (ROWLOCK, READPAST, READCOMMITTEDLOCK)
	WHERE	M.[visible] < getdate()
	AND		M.[expiration] > getdate()
	AND		1 = CASE
					WHEN M.[leaseduntil] is null then 1
					WHEN DATEADD(ms, @leasetolerancemilliseconds, M.[leaseduntil]) < getdate() THEN 1
					ELSE 0
				END
	ORDER
	BY		[priority] DESC,
			[visible] ASC,
			[id] ASC
)
UPDATE	TopCTE WITH (ROWLOCK, READCOMMITTEDLOCK)
SET		[leaseduntil] = DATEADD(ms, @leasemilliseconds, getdate()),
		[leasedat] = getdate(),
		[leasedby] = @leasedby
OUTPUT	inserted.*

";
                    command.Parameters.Add("@leasemilliseconds", SqlDbType.BigInt).Value = 60000;
                    command.Parameters.Add("@leasetolerancemilliseconds", SqlDbType.BigInt).Value = 5000;
                    command.Parameters.Add("@leasedby", SqlDbType.VarChar, 200).Value = "tjek";

                    try
                    {
                        using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
                        {
                            transportMessage = await ExtractTransportMessageFromReader(reader, cancellationToken).ConfigureAwait(false);
                            if (transportMessage == null) return null;

                            var messageId = (long)reader["id"];
                            ApplyTransactionSemantics(context, messageId);
                        }
                    }
                    catch (Exception exception) when (cancellationToken.IsCancellationRequested)
                    {
                        // ADO.NET does not throw the right exception when the task gets cancelled - therefore we need to do this:
                        throw new TaskCanceledException("Receive operation was cancelled", exception);
                    }
                }

                await connection.Complete();
            }

            return transportMessage;
        }

        /// <summary>
        /// Handles automatically renewing a lease for a given message
        /// </summary>
        class AutomaticLeaseRenewer : IDisposable
        {
            readonly string _tableName;
            readonly long _messageId;
            readonly IDbConnectionProvider _connectionProvider;
            readonly long _leaseIntervalMilliseconds;
            Timer _renewTimer;

            public AutomaticLeaseRenewer(string tableName, long messageId, IDbConnectionProvider connectionProvider, long renewIntervalMilliseconds, long leaseIntervalMilliseconds)
            {
                _tableName = tableName;
                _messageId = messageId;
                _connectionProvider = connectionProvider;
                _leaseIntervalMilliseconds = leaseIntervalMilliseconds;

                _renewTimer = new Timer(RenewLease, null, TimeSpan.FromMilliseconds(renewIntervalMilliseconds), TimeSpan.FromMilliseconds(renewIntervalMilliseconds));
            }

            public void Dispose()
            {
                _renewTimer?.Change(TimeSpan.FromMilliseconds(-1), TimeSpan.FromMilliseconds(-1));
                _renewTimer?.Dispose();
                _renewTimer = null;
            }

            async void RenewLease(object state)
            {
                await UpdateLease(_connectionProvider, _tableName, _messageId, _leaseIntervalMilliseconds).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Updates a lease with a new leaseduntil value
        /// </summary>
        /// <param name="connectionProvider">Provider for obtaining a connection</param>
        /// <param name="tableName">Name of the table the messages are stored in</param>
        /// <param name="messageId">Identifier of the message whose lease is being updated</param>
        /// <param name="leaseIntervalMilliseconds">New lease interval in milliseconds. If <c>null</c> the lease will be released</param>
        static async Task UpdateLease(IDbConnectionProvider connectionProvider, string tableName, long messageId, long? leaseIntervalMilliseconds)
        {
            using (var connection = await connectionProvider.GetConnection())
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandType = CommandType.Text;
                    command.CommandText = $@"
UPDATE	{tableName} WITH (ROWLOCK)
SET		leaseduntil =	CASE
							WHEN @leaseintervalmilliseconds IS NULL THEN NULL
							ELSE dateadd(ms, @leaseintervalmilliseconds, getdate())
						END,
		leasedby	=	CASE
							WHEN @leaseintervalmilliseconds IS NULL THEN NULL
							ELSE leasedby
						END,
		leasedat	=	CASE
							WHEN @leaseintervalmilliseconds IS NULL THEN NULL
							ELSE leasedat
						END
WHERE	id = @id
";
                    command.Parameters.Add("@id", SqlDbType.BigInt).Value = messageId;
                    command.Parameters.Add("@leaseintervalmilliseconds", SqlDbType.BigInt).Value = (object)leaseIntervalMilliseconds ?? DBNull.Value;
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                }

                await connection.Complete();
            }
        }

        void ApplyTransactionSemantics(ITransactionContext context, long messageId)
        {
            AutomaticLeaseRenewer renewal = null;

            if (true)
            {
                renewal = new AutomaticLeaseRenewer(_inputQueueName, messageId, _connectionProvider, 45000, 60000);
            }

            context.OnAborted(
                () =>
                {
                    renewal?.Dispose();

                    AsyncHelpers.RunSync(() => UpdateLease(_connectionProvider, _inputQueueName, messageId, null));
                }
            );

            context.OnCommitted(
                async () =>
                {
                    renewal?.Dispose();

                    // Delete the message
                    using (var deleteConnection = await _connectionProvider.GetConnection())
                    {
                        using (var deleteCommand = deleteConnection.CreateCommand())
                        {
                            deleteCommand.CommandType = CommandType.Text;
                            deleteCommand.CommandText = $@"
DELETE
FROM	[{_schema}].[{_inputQueueName}] WITH (ROWLOCK)
WHERE	id = @id
";
                            deleteCommand.Parameters.Add("@id", SqlDbType.BigInt).Value = messageId;
                            deleteCommand.ExecuteNonQuery();
                        }

                        await deleteConnection.Complete();
                    }
                }
            );
        }

        static async Task<TransportMessage> ExtractTransportMessageFromReader(DbDataReader reader, CancellationToken cancellationToken)
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


        public string Address => _inputQueueName;

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

        int GetInitialVisibilityDelay(IDictionary<string, string> headers)
        {
            if (!headers.TryGetValue(Headers.DeferredUntil, out var deferredUntilDateTimeOffsetString))
            {
                return 0;
            }

            var deferredUntilTime = deferredUntilDateTimeOffsetString.ToDateTimeOffset();

            headers.Remove(Headers.DeferredUntil);

            return (int)(deferredUntilTime - _rebusTime.Now).TotalSeconds;
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

        class OutgoingSqlMessage
        {
            public string Schema { get; }
            public string TableName { get; }
            public TransportMessage TransportMessage { get; }

            public OutgoingSqlMessage(string schema, string tableName, TransportMessage transportMessage)
            {
                Schema = schema;
                TableName = tableName;
                TransportMessage = transportMessage;
            }
        }
    }
}