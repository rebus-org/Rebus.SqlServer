using System;
using System.Collections.Concurrent;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Threading;
using Rebus.Transport;

namespace Rebus.SqlServer.Transport
{
	/// <summary>
	/// Similar to <seealso cref="SqlServerTransport"/> but does not maintain an active connection during message processing. Instead a "lease" is acquired for each message and only once "committed" is the message removed from the queue.
	/// <remarks>Note: This also changes the semantics of sending. Sent messages are queued in memory and are not committed to memory until the sender has committed</remarks>
	/// </summary>
    public class SqlServerLeaseTransport : SqlServerTransport
    {
		/// <summary>
		/// Key for storing the outbound message buffer when performing <seealso cref="Send"/>
		/// </summary>
	    public const string OutboundMessageBufferKey = "sql-server-transport-leased-outbound-message-buffer";

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

	    readonly long _leaseIntervalMilliseconds;
	    readonly long _leaseToleranceMilliseconds;
	    readonly bool _automaticLeaseRenewal;
	    readonly long _automaticLeaseRenewalIntervalMilliseconds;

		/// <summary>
		/// Constructor
		/// </summary>
	    public SqlServerLeaseTransport(
			IDbConnectionProvider connectionProvider, 
			string tableName, 
			string inputQueueName, 
			IRebusLoggerFactory rebusLoggerFactory, 
			IAsyncTaskFactory asyncTaskFactory,
			TimeSpan leaseInterval, 
			TimeSpan? leaseTolerance,
			TimeSpan? automaticLeaseRenewalInterval = null
			) : base(connectionProvider, tableName, inputQueueName, rebusLoggerFactory, asyncTaskFactory)
		{
			_leaseIntervalMilliseconds = (long)Math.Ceiling(leaseInterval.TotalMilliseconds);
			_leaseToleranceMilliseconds = (long)Math.Ceiling((leaseTolerance ?? TimeSpan.FromSeconds(15)).TotalMilliseconds);
			if (automaticLeaseRenewalInterval.HasValue == false) {
				_automaticLeaseRenewal = false;
			} else {
				_automaticLeaseRenewal = true;
				_automaticLeaseRenewalIntervalMilliseconds = (long)Math.Ceiling(automaticLeaseRenewalInterval.Value.TotalMilliseconds);
			}
		}

	    /// <summary>
	    /// Sends the given transport message to the specified logical destination address by adding it to the messages table.
	    /// </summary>
	    public override Task Send(string destinationAddress, TransportMessage message, ITransactionContext context) {
			ConcurrentQueue<AddressedTransportMessage> outboundMessageBuffer = GetOutboundMessageBuffer(context);
			outboundMessageBuffer.Enqueue(
				new AddressedTransportMessage() {
					DestinationAddress = GetDestinationAddressToUse(destinationAddress, message),
					Message = message
				}
			);

			return Task.FromResult(0);
	    }

	    /// <summary>
	    /// Handle retrieving a message from the queue, decoding it, and performing any transaction maintenance.
	    /// </summary>
	    /// <param name="context">Tranasction context the receive is operating on</param>
	    /// <param name="cancellationToken">Token to abort processing</param>
	    /// <returns>A <seealso cref="TransportMessage"/> or <c>null</c> if no message can be dequeued</returns>
	    protected override async Task<TransportMessage> ReceiveInternal(ITransactionContext context, CancellationToken cancellationToken) {
			TransportMessage transportMessage = null;

			using (IDbConnection connection = await ConnectionProvider.GetConnection()) {
				using (SqlCommand selectCommand = connection.CreateCommand()) {
					selectCommand.CommandType = CommandType.Text;
					selectCommand.CommandText = $@"
;WITH TopCTE AS (
	SELECT	TOP 1
			[id],
			[headers],
			[body],
			[leaseduntil]
	FROM	{TableName.QualifiedName} M WITH (ROWLOCK, READPAST)
	WHERE	M.[recipient] = @recipient
	AND		M.[visible] < getdate()
	AND		M.[expiration] > getdate()
	AND		1 = CASE
					WHEN M.[leaseduntil] is null then 1
					WHEN DATEADD(ms, @leasetolerancemilliseconds, M.[leaseduntil]) < getdate() THEN 1
					ELSE 0
				END
	ORDER
	BY		[priority] ASC,
			[id] ASC
)
UPDATE	TopCTE WITH (ROWLOCK)
SET		leaseduntil = DATEADD(ms, @leasemilliseconds, getdate())
OUTPUT	inserted.*";
					selectCommand.Parameters.Add("@recipient", SqlDbType.NVarChar, RecipientColumnSize).Value = InputQueueName;
					selectCommand.Parameters.Add("@leasemilliseconds", SqlDbType.BigInt).Value = _leaseIntervalMilliseconds;
					selectCommand.Parameters.Add("@leasetolerancemilliseconds", SqlDbType.BigInt).Value = _leaseToleranceMilliseconds;

					try {
						using (SqlDataReader reader = await selectCommand.ExecuteReaderAsync(cancellationToken)) {
							transportMessage = await ExtractTransportMessageFromReader(reader, cancellationToken);
							if (transportMessage == null) {
								return null;
							}

							Int64 messageId = (Int64)reader["id"];
							ApplyTransactionSemantics(context, messageId);
						}
					} catch (Exception exception) when (cancellationToken.IsCancellationRequested) {
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
	    protected override string AdditionalSchenaModifications(IDbConnection connection) {
			return $@"
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{TableName.Schema}' AND TABLE_NAME = '{TableName.Name}' AND COLUMN_NAME = 'leaseduntil')
BEGIN
	ALTER TABLE {TableName.QualifiedName} ADD leaseduntil datetime2 null
END
";
		}

	    /// <summary>
		/// Responsible for releasing the lease on message failure and removing the message on transaction commit
		/// </summary>
		/// <param name="context">Transaction context of the message processing</param>
		/// <param name="messageId">Identifier of the message currently being processed</param>
	    private void ApplyTransactionSemantics(ITransactionContext context, long messageId) {
			AutomaticLeaseRenewer renewal = null;
			if (_automaticLeaseRenewal == true) {
				renewal = new AutomaticLeaseRenewer(TableName.QualifiedName, messageId, ConnectionProvider, _automaticLeaseRenewalIntervalMilliseconds, _leaseIntervalMilliseconds);
			}

			context.OnAborted(
				async () => {
					renewal?.Dispose();
					await UpdateLease(ConnectionProvider, TableName.QualifiedName, messageId, null);
				}
			);
			context.OnCommitted(
				async () => {
					renewal?.Dispose();

					// Delete the message
					using (IDbConnection deleteConnection = await ConnectionProvider.GetConnection()) {
						using (SqlCommand deleteCommand = deleteConnection.CreateCommand()) {
							deleteCommand.CommandType = CommandType.Text;
							deleteCommand.CommandText = $@"
DELETE
FROM	{TableName.QualifiedName} WITH (ROWLOCK)
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

		/// <summary>
		/// Gets the outbound message buffer for sending of messages
		/// </summary>
		/// <param name="context">Transaction context containing the message bufffer</param>
	    private ConcurrentQueue<AddressedTransportMessage> GetOutboundMessageBuffer(ITransactionContext context)
		{
			return context.Items.GetOrAdd(OutboundMessageBufferKey, key => {
				context.OnCommitted(
					async () => {
						object messageBufferRaw = null;
						if (context.Items.TryGetValue(OutboundMessageBufferKey, out messageBufferRaw) == false) {
							throw new Exception("Unable to extract message buffer from transaction context");
						}

						ConcurrentQueue<AddressedTransportMessage> messageBuffer = messageBufferRaw as ConcurrentQueue<AddressedTransportMessage>;
						using (IDbConnection connection = await ConnectionProvider.GetConnection()) {
							while (messageBuffer.IsEmpty == false) {
								AddressedTransportMessage addressed = null;
								if (messageBuffer.TryDequeue(out addressed) == false) {
									break;
								}

								await InnerSend(addressed.DestinationAddress, addressed.Message, connection);
							}

							await connection.Complete();
						}
					}
				);
				return new ConcurrentQueue<AddressedTransportMessage>();
			}
			) as ConcurrentQueue<AddressedTransportMessage>;
		}

		/// <summary>
		/// Updates a lease with a new leaseduntil value
		/// </summary>
		/// <param name="connectionProvider">Provider for obtaining a connection</param>
		/// <param name="tableName">Name of the table the messages are stored in</param>
		/// <param name="messageId">Identifier of the message whose lease is being updated</param>
		/// <param name="leaseIntervalMilliseconds">New lease interval in milliseconds. If <c>null</c> the lease will be released</param>
		/// <returns></returns>
		private static async Task UpdateLease(IDbConnectionProvider connectionProvider, string tableName, long messageId, long? leaseIntervalMilliseconds)
		{
			using (IDbConnection connection = await connectionProvider.GetConnection()) {
				using (SqlCommand command = connection.CreateCommand()) {
					command.CommandType = CommandType.Text;
					command.CommandText = $@"
UPDATE	{tableName} WITH (ROWLOCK)
SET		leaseduntil =	CASE
							WHEN @leaseintervalmilliseconds IS NULL then NULL
							ELSE dateadd(ms, @leaseintervalmilliseconds, getdate())
						END
WHERE	id = @id
";
					command.Parameters.Add("@id", SqlDbType.BigInt).Value = messageId;
					command.Parameters.Add("@leaseintervalmilliseconds", SqlDbType.BigInt).Value = (object)leaseIntervalMilliseconds ?? DBNull.Value;
					await command.ExecuteNonQueryAsync();
				}

				await connection.Complete();
			}
		}

		/// <summary>
		/// Handles automatically renewing a lease for a given message
		/// </summary>
		private class AutomaticLeaseRenewer : IDisposable {
			private readonly string _tableName;
			private readonly long _messageId;
			private readonly IDbConnectionProvider _connectionProvider;
			private readonly long _leaseIntervalMilliseconds;
			private readonly Timer _renewTimer;

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
			}

			private async void RenewLease(object state)
			{
				await SqlServerLeaseTransport.UpdateLease(_connectionProvider, _tableName, _messageId, _leaseIntervalMilliseconds);
			}
		}

		private class AddressedTransportMessage {
		    public string DestinationAddress { get; set; }
		    public TransportMessage Message { get; set; }
	    }
	}
}
