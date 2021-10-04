using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Rebus.Bus;
using Rebus.Serialization;
using Rebus.Transport;

namespace Rebus.SqlServer.Outbox
{
    /// <summary>
    /// Outbox implementation that uses a table in SQL Server to store the necessary outbox information
    /// </summary>
    public class SqlServerOutboxStorage : IOutboxStorage, IInitializable
    {
        private readonly Func<ITransactionContext, IDbConnection> _connectionProvider;
        private readonly TableName _tableName;
        private static readonly HeaderSerializer HeaderSerializer = new HeaderSerializer();

        /// <summary>
        /// Creates the outbox storage
        /// </summary>
        public SqlServerOutboxStorage(Func<ITransactionContext, IDbConnection> connectionProvider, TableName tableName)
        {
            _connectionProvider = connectionProvider;
            _tableName = tableName;
        }

        /// <summary>
        /// Initializes the outbox storage
        /// </summary>
        public void Initialize()
        {
            async Task InitializeAsync()
            {
                using var scope = new RebusTransactionScope();
                using var connection = _connectionProvider(scope.TransactionContext);

                if (connection.GetTableNames().Contains(_tableName)) return;

                try
                {
                    using var command = connection.CreateCommand();

                    command.CommandText = $@"
CREATE TABLE {_tableName} (
    [Id] bigint identity(1,1),
    [MessageId] nvarchar(255) null,
    [SourceQueue] nvarchar(255) null,
    [DestinationAddress] nvarchar(255) not null,
    [Headers] nvarchar(max) null,
    [Body] varbinary(max) null,
    primary key ([Id])
)
";

                    await command.ExecuteNonQueryAsync();

                    await connection.Complete();
                }
                catch (Exception)
                {
                    if (connection.GetTableNames().Contains(_tableName)) return;

                    throw;
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
        public async Task Save(string messageId, IEnumerable<AbstractRebusTransport.OutgoingMessage> outgoingMessages)
        {
            throw new System.NotImplementedException();
        }

        /// <summary>
        /// Stores the given <paramref name="outgoingMessages"/> to be sent.
        /// </summary>
        public async Task Save(IEnumerable<AbstractRebusTransport.OutgoingMessage> outgoingMessages)
        {
            using var scope = new RebusTransactionScope();
            using var connection = _connectionProvider(scope.TransactionContext);

            async Task Insert(AbstractRebusTransport.OutgoingMessage msg)
            {
                using var command = connection.CreateCommand();

                var transportMessage = msg.TransportMessage;
                var body = msg.TransportMessage.Body;
                var headers = SerializeHeaders(transportMessage.Headers);

                command.CommandText = $"INSERT INTO {_tableName} ([DestinationAddress], [Headers], [Body]) VALUES (@destinationAddress, @headers, @body)";
                command.Parameters.Add("destinationAddress", SqlDbType.NVarChar, 255).Value = msg.DestinationAddress;
                command.Parameters.Add("headers", SqlDbType.NVarChar, GetLength(headers)).Value = headers;
                command.Parameters.Add("body", SqlDbType.VarBinary, GetLength(headers)).Value = body;

                await command.ExecuteNonQueryAsync();
            }

            foreach (var message in outgoingMessages)
            {
                await Insert(message);
            }

            await connection.Complete();
            await scope.CompleteAsync();
        }

        static int GetLength(string str) => str.Length;

        static string SerializeHeaders(Dictionary<string, string> headers) => HeaderSerializer.SerializeToString(headers);
    }
}
