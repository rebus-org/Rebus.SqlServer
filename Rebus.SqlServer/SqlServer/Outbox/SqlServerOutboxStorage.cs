﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Rebus.Bus;
using Rebus.Serialization;
using Rebus.Transport;

namespace Rebus.SqlServer.Outbox;

/// <summary>
/// Outbox implementation that uses a table in SQL Server to store the necessary outbox information
/// </summary>
public class SqlServerOutboxStorage : IOutboxStorage, IInitializable
{
    static readonly HeaderSerializer HeaderSerializer = new();
    readonly Func<ITransactionContext, Task<IDbConnection>> _connectionProvider;
    readonly TableName _tableName;

    /// <summary>
    /// Creates the outbox storage
    /// </summary>
    public SqlServerOutboxStorage(Func<ITransactionContext, Task<IDbConnection>> connectionProvider, TableName tableName)
    {
        _connectionProvider = connectionProvider ?? throw new ArgumentNullException(nameof(connectionProvider));
        _tableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
    }

    /// <summary>
    /// Initializes the outbox storage
    /// </summary>
    public void Initialize()
    {
        async Task InitializeAsync()
        {
            using var scope = new RebusTransactionScope();
            using var connection = await _connectionProvider(scope.TransactionContext);

            if (connection.GetTableNames().Contains(_tableName)) return;

            try
            {
                using var command = connection.CreateCommand();

                command.CommandText = $@"
CREATE TABLE {_tableName} (
    [Id] bigint identity(1,1),
    [CorrelationId] nvarchar(16) null,
    [MessageId] nvarchar(255) null,
    [SourceQueue] nvarchar(255) null,
    [DestinationAddress] nvarchar(255) not null,
    [Headers] nvarchar(max) null,
    [Body] varbinary(max) null,
    [Sent] bit not null default(0),
    primary key ([Id])
)
";

                await command.ExecuteNonQueryAsync();

                await connection.Complete();
            }
            catch (Exception)
            {
                if (!connection.GetTableNames().Contains(_tableName))
                {
                    throw;
                }
            }

            await scope.CompleteAsync();
        }

        AsyncHelpers.RunSync(InitializeAsync);
    }

    /// <summary>
    /// Stores the given <paramref name="outgoingMessages"/> as being the result of processing message with ID <paramref name="messageId"/>
    /// in the queue of this particular endpoint. If <paramref name="outgoingMessages"/> is an empty sequence, a note is made of the fact
    /// that the message with ID <paramref name="messageId"/> has been processed.
    /// </summary>
    public async Task Save(IEnumerable<OutgoingTransportMessage> outgoingMessages, string messageId = null, string sourceQueue = null, string correlationId = null)
    {
        if (outgoingMessages == null) throw new ArgumentNullException(nameof(outgoingMessages));

        await InnerSave(outgoingMessages, messageId, sourceQueue, correlationId);
    }

    /// <summary>
    /// Stores the given <paramref name="outgoingMessages"/> using the given <paramref name="dbConnection"/>.
    /// </summary>
    public async Task Save(IEnumerable<OutgoingTransportMessage> outgoingMessages, IDbConnection dbConnection)
    {
        if (outgoingMessages == null) throw new ArgumentNullException(nameof(outgoingMessages));
        if (dbConnection == null) throw new ArgumentNullException(nameof(dbConnection));

        await SaveUsingConnection(dbConnection, outgoingMessages);
    }

    /// <inheritdoc />
    public async Task<OutboxMessageBatch> GetNextMessageBatch(string correlationId = null, int maxMessageBatchSize = 100)
    {
        return await InnerGetMessageBatch(maxMessageBatchSize, correlationId);
    }

    async Task<OutboxMessageBatch> InnerGetMessageBatch(int maxMessageBatchSize, string correlationId)
    {
        if (maxMessageBatchSize <= 0)
        {
            throw new ArgumentException(
                $"Cannot retrieve {maxMessageBatchSize} messages - please pass in a value >= 1",
                nameof(maxMessageBatchSize));
        }

        // no 'using' here, because this will be passed to the outbox message batch
        var scope = new RebusTransactionScope();

        try
        {
            // no 'using' here either, because this will be passed to the outbox message batch
            var connection = await _connectionProvider(scope.TransactionContext);

            // this must be done when cleanining up
            void Dispose()
            {
                connection.Dispose();
                scope.Dispose();
            }

            try
            {
                var messages = await GetOutboxMessages(connection, maxMessageBatchSize, correlationId);

                // bail out if no messages were found
                if (!messages.Any()) return OutboxMessageBatch.Empty(Dispose);

                // define what it means to complete the batch
                async Task Complete()
                {
                    await CompleteMessages(connection, messages);
                    await connection.Complete();
                    await scope.CompleteAsync();
                }

                return new OutboxMessageBatch(Complete, messages, Dispose);
            }
            catch (Exception)
            {
                connection.Dispose();
                throw;
            }
        }
        catch (Exception)
        {
            scope.Dispose();
            throw;
        }
    }

    async Task InnerSave(IEnumerable<OutgoingTransportMessage> outgoingMessages, string messageId, string sourceQueue, string correlationId)
    {
        using var scope = new RebusTransactionScope();
        using var connection = await _connectionProvider(scope.TransactionContext);

        await SaveUsingConnection(connection, outgoingMessages, messageId, sourceQueue, correlationId);

        await connection.Complete();
        await scope.CompleteAsync();
    }

    async Task SaveUsingConnection(IDbConnection connection, IEnumerable<OutgoingTransportMessage> outgoingMessages, string messageId = null, string sourceQueue = null, string correlationId = null)
    {
        foreach (var message in outgoingMessages)
        {
            using var command = connection.CreateCommand();

            var transportMessage = message.TransportMessage;
            var body = message.TransportMessage.Body;
            var headers = SerializeHeaders(transportMessage.Headers);

            command.CommandText = $"INSERT INTO {_tableName} ([CorrelationId], [MessageId], [SourceQueue], [DestinationAddress], [Headers], [Body]) VALUES (@correlationId, @messageId, @sourceQueue, @destinationAddress, @headers, @body)";
            command.Parameters.Add("correlationId", SqlDbType.NVarChar, 16).Value = (object)correlationId ?? DBNull.Value;
            command.Parameters.Add("messageId", SqlDbType.NVarChar, 255).Value = (object)messageId ?? DBNull.Value;
            command.Parameters.Add("sourceQueue", SqlDbType.NVarChar, 255).Value = (object)sourceQueue ?? DBNull.Value;
            command.Parameters.Add("destinationAddress", SqlDbType.NVarChar, 255).Value = message.DestinationAddress;
            command.Parameters.Add("headers", SqlDbType.NVarChar, headers.Length.RoundUpToNextPowerOfTwo()).Value = headers;
            command.Parameters.Add("body", SqlDbType.VarBinary, body.Length.RoundUpToNextPowerOfTwo()).Value = body;

            await command.ExecuteNonQueryAsync();
        }
    }

    async Task CompleteMessages(IDbConnection connection, IEnumerable<OutboxMessage> messages)
    {
        using var command = connection.CreateCommand();

        var ids = messages.Select(m => m.Id).ToList();
        var idString = string.Join(", ", ids);

        command.CommandText = $"UPDATE {_tableName} SET [Sent] = 1 WHERE [Id] IN ({idString})";

        await command.ExecuteNonQueryAsync();
    }

    async Task<List<OutboxMessage>> GetOutboxMessages(IDbConnection connection, int maxMessageBatchSize, string correlationId)
    {
        using var command = connection.CreateCommand();

        if (correlationId != null)
        {
            command.CommandText = $"SELECT TOP {maxMessageBatchSize} [Id], [DestinationAddress], [Headers], [Body] FROM {_tableName} WITH (UPDLOCK, READPAST) WHERE [CorrelationId] = @correlationId [Sent] = 0 ORDER BY [Id]";
            command.Parameters.Add("correlationId", SqlDbType.NVarChar, 16).Value = correlationId;
        }
        else
        {
            command.CommandText = $"SELECT TOP {maxMessageBatchSize} [Id], [DestinationAddress], [Headers], [Body] FROM {_tableName} WITH (UPDLOCK, READPAST) WHERE [Sent] = 0 ORDER BY [Id]";
        }

        using var reader = await command.ExecuteReaderAsync();

        var messages = new List<OutboxMessage>();

        while (await reader.ReadAsync())
        {
            var id = (long)reader["id"];
            var destinationAddress = (string)reader["destinationAddress"];
            var headers = HeaderSerializer.DeserializeFromString((string)reader["headers"]);
            var body = (byte[])reader["body"];
            messages.Add(new OutboxMessage(id, destinationAddress, headers, body));
        }

        return messages;
    }

    static string SerializeHeaders(Dictionary<string, string> headers) => HeaderSerializer.SerializeToString(headers);
}
