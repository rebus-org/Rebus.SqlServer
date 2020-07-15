using System.Collections.Generic;
using System.Threading.Tasks;
using Rebus.Auditing.Sagas;
using Rebus.Logging;
using Rebus.Sagas;
using Rebus.Serialization;
using Rebus.SqlServer.Sagas;
using Rebus.Tests.Contracts.Sagas;

namespace Rebus.SqlServer.Tests.Sagas
{
    public class SqlServerSnapshotStorageFactory : ISagaSnapshotStorageFactory
    {
        const string TableName = "SagaSnapshots";

        public SqlServerSnapshotStorageFactory()
        {
            SqlTestHelper.DropTable(TableName);
        }

        public ISagaSnapshotStorage Create()
        {
            var consoleLoggerFactory = new ConsoleLoggerFactory(true);
            var connectionProvider = new DbConnectionProvider(SqlTestHelper.ConnectionString, consoleLoggerFactory);

            var snapperino = new SqlServerSagaSnapshotStorage(connectionProvider, TableName, consoleLoggerFactory);

            snapperino.EnsureTableIsCreated();

            return snapperino;
        }

        public IEnumerable<SagaDataSnapshot> GetAllSnapshots()
        {
            return LoadStoredCopies(new DbConnectionProvider(SqlTestHelper.ConnectionString, new ConsoleLoggerFactory(true)), TableName).Result;
        }

        static async Task<List<SagaDataSnapshot>> LoadStoredCopies(DbConnectionProvider connectionProvider, string tableName)
        {
            var storedCopies = new List<SagaDataSnapshot>();

            using (var connection = await connectionProvider.GetConnection())
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"SELECT * FROM [{tableName}]";

                    using var reader = command.ExecuteReader();
                    while (await reader.ReadAsync())
                    {
                        var sagaData = (ISagaData)new ObjectSerializer().DeserializeFromString((string)reader["data"]);
                        var metadata = new HeaderSerializer().DeserializeFromString((string)reader["metadata"]);

                        storedCopies.Add(new SagaDataSnapshot { SagaData = sagaData, Metadata = metadata });
                    }
                }

                await connection.Complete();
            }
            return storedCopies;
        }
    
    }
}
