using System;
using System.Collections.Concurrent;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Rebus.Config;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Threading;
using Rebus.Time;
using Rebus.Transport;

namespace Rebus.SqlServer.Transport
{
    /// <summary>
    /// Similar to <seealso cref="SqlServerTransport"/> but does not maintain an active connection during message processing. Instead a "lease" is acquired for each message and only once "committed" is the message removed from the queue.
    /// <remarks>Note: This also changes the semantics of sending. Sent messages are queued in memory and are not committed to memory until the sender has committed</remarks>
    /// </summary>
    public class SqlServerLeaseTransport : SqlServerTransport
    {
        static readonly Task CompletedResult = Task.FromResult(0);

        /// <summary>
        /// Key for storing the outbound message buffer when performing <seealso cref="Send"/>
        /// </summary>
        public const string OutboundMessageBufferKey = "sql-server-transport-leased-outbound-message-buffer";

        /// <summary>
        /// Size of the leasedby column
        /// </summary>
        public const int LeasedByColumnSize = 200;

        /// <summary>
        /// If not specified the default time messages are leased for
        /// </summary>
        public static readonly TimeSpan DefaultLeaseTime = TimeSpan.FromMinutes(5);

        /// <summary>
        /// If not specified the amount of tolerance workers will allow a message which has already been leased
        /// </summary>
        public static readonly TimeSpan DefaultLeaseTolerance = TimeSpan.FromSeconds(30);

        /// <summary>
        /// If not specified the amount of time the workers will automatically renew leases for actively handled messages
        /// </summary>
        public static readonly TimeSpan DefaultLeaseAutomaticRenewal = TimeSpan.FromSeconds(150);

        readonly TimeSpan _leaseInterval;
        readonly TimeSpan _leaseTolerance;
        readonly bool _automaticLeaseRenewal;
        readonly TimeSpan _automaticLeaseRenewalInterval;
        readonly Func<string> _leasedByFactory;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="connectionProvider">A <see cref="IDbConnection"/> to obtain a database connection</param>
        /// <param name="inputQueueName">Name of the queue this transport is servicing</param>
        /// <param name="rebusLoggerFactory">A <seealso cref="IRebusLoggerFactory"/> for building loggers</param>
        /// <param name="asyncTaskFactory">A <seealso cref="IAsyncTaskFactory"/> for creating periodic tasks</param>
        /// <param name="rebusTime">A <seealso cref="IRebusTime"/> to provide the current time</param>
        /// <param name="leaseInterval">Interval of time messages are leased for</param>
        /// <param name="leaseTolerance">Buffer to allow lease overruns by</param>
        /// <param name="leasedByFactory">Factory for generating a string which identifies who has leased a message (eg. A hostname)</param>
        /// <param name="options">Additional options</param>
        public SqlServerLeaseTransport(
            IDbConnectionProvider connectionProvider,
            string inputQueueName,
            IRebusLoggerFactory rebusLoggerFactory,
            IAsyncTaskFactory asyncTaskFactory,
            IRebusTime rebusTime,
            TimeSpan leaseInterval,
            TimeSpan? leaseTolerance,
            Func<string> leasedByFactory,
            SqlServerLeaseTransportOptions options
            ) : base(connectionProvider, inputQueueName, rebusLoggerFactory, asyncTaskFactory, rebusTime, options)
        {
            _leasedByFactory = leasedByFactory;
            _leaseInterval = leaseInterval;
            _leaseTolerance = leaseTolerance ?? TimeSpan.FromSeconds(15);

            var automaticLeaseRenewalInterval = options.LeaseAutoRenewInterval;

            if (!automaticLeaseRenewalInterval.HasValue)
            {
                _automaticLeaseRenewal = false;
            }
            else
            {
                _automaticLeaseRenewal = true;
                _automaticLeaseRenewalInterval = automaticLeaseRenewalInterval.Value;
            }
        }

        /// <summary>
        /// Sends the given transport message to the specified logical destination address by adding it to the messages table.
        /// </summary>
        public override Task Send(string destinationAddress, TransportMessage message, ITransactionContext context)
        {
            var outboundMessageBuffer = GetOutboundMessageBuffer(context);

            outboundMessageBuffer.Enqueue(
                new AddressedTransportMessage
                {
                    DestinationAddress = GetDestinationAddressToUse(destinationAddress, message),
                    Message = message
                }
            );

            return CompletedResult;
        }

        /// <summary>
        /// Handle retrieving a message from the queue, decoding it, and performing any transaction maintenance.
        /// </summary>
        /// <param name="context">Tranasction context the receive is operating on</param>
        /// <param name="cancellationToken">Token to abort processing</param>
        /// <returns>A <seealso cref="TransportMessage"/> or <c>null</c> if no message can be dequeued</returns>
        protected override async Task<TransportMessage> ReceiveInternal(ITransactionContext context, CancellationToken cancellationToken)
        {
            TransportMessage transportMessage = null;

            using (var connection = await ConnectionProvider.GetConnection())
            {
                using (var selectCommand = connection.CreateCommand())
                {
                    selectCommand.CommandType = CommandType.Text;
                    selectCommand.CommandText = $@"
;WITH TopCTE AS (
	SELECT	TOP 1
			[id],
			[headers],
			[body],
			[leasedat],
			[leaseduntil],
			[leasedby]
	FROM	{ReceiveTableName.QualifiedName} M WITH (ROWLOCK, READPAST, READCOMMITTEDLOCK)
	WHERE	M.[visible] < sysdatetimeoffset()
	AND		M.[expiration] > sysdatetimeoffset()
	AND		1 = CASE
					WHEN M.[leaseduntil] is null then 1
					WHEN DATEADD(ms, @leasetolerancemilliseconds, DATEADD(ss, @leasetolerancetotalseconds, M.[leaseduntil])) < sysdatetimeoffset() THEN 1
					ELSE 0
				END
	ORDER
	BY		[priority] DESC,
			[visible] ASC,
			[id] ASC
)
UPDATE	TopCTE WITH (ROWLOCK, READCOMMITTEDLOCK)
SET		[leaseduntil] = DATEADD(ms, @leasemilliseconds, DATEADD(ss, @leasetotalseconds, sysdatetimeoffset())),
		[leasedat] = sysdatetimeoffset(),
		[leasedby] = @leasedby
OUTPUT	inserted.*";
                    selectCommand.Parameters.Add("@leasetotalseconds", SqlDbType.Int).Value = (int)_leaseInterval.TotalSeconds;
                    selectCommand.Parameters.Add("@leasemilliseconds", SqlDbType.Int).Value = _leaseInterval.Milliseconds;
                    selectCommand.Parameters.Add("@leasetolerancetotalseconds", SqlDbType.Int).Value = (int)_leaseTolerance.TotalSeconds;
                    selectCommand.Parameters.Add("@leasetolerancemilliseconds", SqlDbType.Int).Value = _leaseTolerance.Milliseconds;
                    selectCommand.Parameters.Add("@leasedby", SqlDbType.VarChar, LeasedByColumnSize).Value = _leasedByFactory();

                    try
                    {
                        using (var reader = await selectCommand.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
                        {
                            transportMessage = await ExtractTransportMessageFromReader(reader, cancellationToken).ConfigureAwait(false);
                            if (transportMessage == null) return null;

                            var messageId = (long)reader["id"];
                            ApplyTransactionSemantics(context, messageId, cancellationToken);
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
        /// Provides an oppurtunity for derived implementations to also update the schema
        /// </summary>
        /// <param name="tableName"></param>
        protected override string AdditionalSchemaModifications(TableName tableName)
        {
            var receiveIndexName = $"IDX_RECEIVE_LEASE_{tableName.Schema}_{tableName.Name}";
            var deleteIndexName = $"IDX_DELETE_LEASE_{tableName.Schema}_{tableName.Name}";

            return $@"
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{tableName.Schema}' AND TABLE_NAME = '{tableName.Name}' AND COLUMN_NAME = 'leaseduntil')
BEGIN
	ALTER TABLE {tableName.QualifiedName} ADD leaseduntil datetimeoffset null
END

----

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{tableName.Schema}' AND TABLE_NAME = '{tableName.Name}' AND COLUMN_NAME = 'leasedby')
BEGIN
	ALTER TABLE {tableName.QualifiedName} ADD leasedby nvarchar({LeasedByColumnSize}) null
END


----

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{tableName.Schema}' AND TABLE_NAME = '{tableName.Name}' AND COLUMN_NAME = 'leasedat')
BEGIN
	ALTER TABLE {tableName.QualifiedName} ADD leasedat datetimeoffset null
END

----

-- Drop the V0 Receive Index
-- v0 index was (Priority, Visible, Expiration, LeasedUntil, Id)
-- We can find this by looking for the index with priority as is_descending_key = 0
IF EXISTS (SELECT 1 FROM sys.indexes I JOIN sys.index_columns IC ON I.object_id = OBJECT_ID('{tableName.QualifiedName}') AND I.name = '{receiveIndexName}' AND IC.object_id = I.object_id AND IC.index_id = I.index_id JOIN sys.columns C ON C.object_id = IC.object_id AND C.column_id = IC.column_id AND C.name = 'priority' and IC.is_descending_key = 0)
BEGIN
    DROP INDEX [{receiveIndexName}] ON {tableName.QualifiedName}
END

----

-- V1 Index: (Priority DESC, Visible, Id, Expiration, LeasedUntil)
IF NOT EXISTS (SELECT 1 FROM sys.indexes I JOIN sys.objects O ON I.name = '{receiveIndexName}' AND I.object_id = o.object_id and o.schema_id = SCHEMA_ID('{tableName.Schema}'))
BEGIN
	CREATE NONCLUSTERED INDEX [{receiveIndexName}] ON {tableName.QualifiedName}
	(
		[priority] DESC,
		[visible] ASC,
		[id] ASC,
		[expiration] ASC,
		[leaseduntil] ASC
	)
END

----

IF NOT EXISTS (SELECT 1 FROM sys.indexes I JOIN sys.objects O ON I.name = '{deleteIndexName}' AND I.object_id = o.object_id and o.schema_id = SCHEMA_ID('{tableName.Schema}'))
BEGIN
	CREATE NONCLUSTERED INDEX [{deleteIndexName}] ON {tableName.QualifiedName}
	(
		[id] ASC
	)
END
";
        }

        /// <summary>
        /// Responsible for releasing the lease on message failure and removing the message on transaction commit
        /// </summary>
        /// <param name="context">Transaction context of the message processing</param>
        /// <param name="messageId">Identifier of the message currently being processed</param>
        /// <param name="cancellationToken">Token to abort processing</param>
        private void ApplyTransactionSemantics(ITransactionContext context, long messageId, CancellationToken cancellationToken)
        {
            AutomaticLeaseRenewer renewal = null;
            if (_automaticLeaseRenewal == true)
            {
                renewal = new AutomaticLeaseRenewer(
                    this, ReceiveTableName.QualifiedName, messageId, ConnectionProvider, _automaticLeaseRenewalInterval, _leaseInterval, cancellationToken);
            }

            context.OnAborted(
                ctx =>
                {
                    renewal?.Dispose();
                    try
                    {
                        AsyncHelpers.RunSync(() => UpdateLease(ConnectionProvider, ReceiveTableName.QualifiedName, messageId, null, cancellationToken));
                    }
                    catch (Exception ex)
                    {
                        Log.Error(ex, "While Resetting Lease");
                    }
                }
            );

            context.OnCommitted(
                async ctx =>
                {
                    renewal?.Dispose();
                    try
                    {
                        await DeleteMessage(messageId, cancellationToken);
                    }
                    catch (Exception ex)
                    {
                        Log.Error(ex, "While Deleteing Message");
                    }
                }
            );
        }

        /// <summary>
        /// Responsible for deleating the message on transaction commit
        /// </summary>
        /// <param name="messageId">Identifier of the message currently being processed</param>
        /// <param name="cancellationToken">Token to abort processing</param>
        protected virtual async Task DeleteMessage(long messageId, CancellationToken cancellationToken)
        {
            // Delete the message
            using (var deleteConnection = await ConnectionProvider.GetConnection())
            {
                using (var deleteCommand = deleteConnection.CreateCommand())
                {
                    deleteCommand.CommandType = CommandType.Text;
                    deleteCommand.CommandText = $@"
DELETE
FROM	{ReceiveTableName.QualifiedName} WITH (ROWLOCK)
WHERE	id = @id
";
                    deleteCommand.Parameters.Add("@id", SqlDbType.BigInt).Value = messageId;
                    await deleteCommand.ExecuteNonQueryAsync(cancellationToken);
                }

                await deleteConnection.Complete();
            }
        }

        /// <summary>
        /// Gets the outbound message buffer for sending of messages
        /// </summary>
        /// <param name="context">Transaction context containing the message bufffer</param>
        ConcurrentQueue<AddressedTransportMessage> GetOutboundMessageBuffer(ITransactionContext context)
        {
            return context.GetOrAdd(OutboundMessageBufferKey, () =>
                {
                    var outgoingMessages = new ConcurrentQueue<AddressedTransportMessage>();

                    async Task SendOutgoingMessages(ITransactionContext _)
                    {
                        using (var connection = await ConnectionProvider.GetConnection())
                        {
                            while (outgoingMessages.IsEmpty == false)
                            {
                                if (outgoingMessages.TryDequeue(out var addressed) == false)
                                {
                                    break;
                                }

                                await InnerSend(addressed.DestinationAddress, addressed.Message, connection);
                            }

                            await connection.Complete();
                        }
                    }

                    context.OnCommitted(SendOutgoingMessages);

                    return outgoingMessages;
                }
            );
        }

        /// <summary>
        /// Updates a lease with a new leaseduntil value
        /// </summary>
        /// <param name="connectionProvider">Provider for obtaining a connection</param>
        /// <param name="tableName">Name of the table the messages are stored in</param>
        /// <param name="messageId">Identifier of the message whose lease is being updated</param>
        /// <param name="leaseInterval">New lease interval. If <c>null</c> the lease will be released</param>
        /// <param name="cancellationToken">Token to abort processing</param>
        protected virtual async Task UpdateLease(IDbConnectionProvider connectionProvider, string tableName, long messageId, TimeSpan? leaseInterval, CancellationToken cancellationToken)
        {
            using (var connection = await connectionProvider.GetConnection())
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandType = CommandType.Text;

                    if (leaseInterval.HasValue)
                    {
                        command.CommandText = $@"
UPDATE	{tableName} WITH (ROWLOCK)
SET		leaseduntil =	dateadd(ms, @leaseintervalmilliseconds, dateadd(ss, @leaseintervaltotalseconds, sysdatetimeoffset())),
		leasedby	=	leasedby,
		leasedat	=	leasedat
WHERE	id = @id
";
                        command.Parameters.Add("@id", SqlDbType.BigInt).Value = messageId;
                        command.Parameters.Add("@leaseintervaltotalseconds", SqlDbType.Int).Value = (int)leaseInterval.Value.TotalSeconds;
                        command.Parameters.Add("@leaseintervalmilliseconds", SqlDbType.Int).Value = leaseInterval.Value.Milliseconds;
                    }
                    else
                    {
                        command.CommandText = $@"
UPDATE	{tableName} WITH (ROWLOCK)
SET		leaseduntil =	NULL,
		leasedby	=	NULL,
		leasedat	=	NULL
WHERE	id = @id
";
                        command.Parameters.Add("@id", SqlDbType.BigInt).Value = messageId;
                    }

                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }

                await connection.Complete();
            }
        }

        /// <summary>
        /// Handles automatically renewing a lease for a given message
        /// </summary>
        class AutomaticLeaseRenewer : IDisposable
        {
            private readonly SqlServerLeaseTransport _serverLeaseTransport;
            readonly string _tableName;
            readonly long _messageId;
            readonly IDbConnectionProvider _connectionProvider;
            readonly TimeSpan _leaseInterval;
            readonly CancellationToken _cancellationToken;
            Timer _renewTimer;

            public AutomaticLeaseRenewer(SqlServerLeaseTransport serverLeaseTransport, string tableName, long messageId, IDbConnectionProvider connectionProvider, TimeSpan renewInterval, TimeSpan leaseInterval, CancellationToken cancellationToken)
            {
                _serverLeaseTransport = serverLeaseTransport;
                _tableName = tableName;
                _messageId = messageId;
                _connectionProvider = connectionProvider;
                _leaseInterval = leaseInterval;
                _cancellationToken = cancellationToken;
                _renewTimer = new Timer(RenewLease, null, renewInterval, renewInterval);
            }


            public void Dispose()
            {
                _renewTimer?.Change(TimeSpan.FromMilliseconds(-1), TimeSpan.FromMilliseconds(-1));
                _renewTimer?.Dispose();
                _renewTimer = null;
            }

            async void RenewLease(object state)
            {
                try
                {
                    await _serverLeaseTransport.UpdateLease(_connectionProvider, _tableName, _messageId, _leaseInterval, _cancellationToken);
                }
                catch (Exception ex)
                {
                    _serverLeaseTransport.Log.Error(ex, "While Renewing Lease");
                }
            }
        }

        class AddressedTransportMessage
        {
            public string DestinationAddress { get; set; }
            public TransportMessage Message { get; set; }
        }
    }
}
