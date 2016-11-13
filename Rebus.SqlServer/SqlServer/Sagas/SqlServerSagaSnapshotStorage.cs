using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Rebus.Auditing.Sagas;
using Rebus.Logging;
using Rebus.Sagas;
using Rebus.Serialization;

namespace Rebus.SqlServer.Sagas
{
    /// <summary>
    /// Implementation of <see cref="ISagaSnapshotStorage"/> that uses a table in SQL Server to store saga snapshots
    /// </summary>
    public class SqlServerSagaSnapshotStorage : ISagaSnapshotStorage
    {
        readonly IDbConnectionProvider _connectionProvider;
        readonly TableName _tableName;
        readonly ILog _log;

        static readonly ObjectSerializer DataSerializer = new ObjectSerializer();
        static readonly HeaderSerializer MetadataSerializer = new HeaderSerializer();

        /// <summary>
        /// Constructs the snapshot storage
        /// </summary>
        public SqlServerSagaSnapshotStorage(IDbConnectionProvider connectionProvider, string tableName, IRebusLoggerFactory rebusLoggerFactory)
        {
            _log = rebusLoggerFactory.GetCurrentClassLogger();
            _connectionProvider = connectionProvider;
            _tableName = TableName.Parse(tableName);
        }

        /// <summary>
        /// Creates the subscriptions table if necessary
        /// </summary>
        public void EnsureTableIsCreated()
        {
            using (var connection = _connectionProvider.GetConnection().Result)
            {
                var tableNames = connection.GetTableNames();
                
                if (tableNames.Contains(_tableName))
                {
                    return;
                }

                _log.Info($"Table '{_tableName.QualifiedName}' does not exist - it will be created now");

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{_tableName.Schema}')
	EXEC('CREATE SCHEMA {_tableName.Schema}')

----

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{_tableName.Schema}' AND TABLE_NAME = '{_tableName.Name}')
    CREATE TABLE {_tableName.QualifiedName} (
	    [id] [uniqueidentifier] NOT NULL,
	    [revision] [int] NOT NULL,
	    [data] [nvarchar](max) NOT NULL,
	    [metadata] [nvarchar](max) NOT NULL,
        CONSTRAINT [PK_{_tableName.Schema}_{_tableName.Name}] PRIMARY KEY CLUSTERED 
        (
	        [id] ASC,
            [revision] ASC
        )
    )

";
                    command.ExecuteNonQuery();
                }

                connection.Complete();
            }
        }

        /// <summary>
        /// Saves a snapshot of the saga data along with the given metadata
        /// </summary>
        public async Task Save(ISagaData sagaData, Dictionary<string, string> sagaAuditMetadata)
        {
            using (var connection = await _connectionProvider.GetConnection())
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        $@"

INSERT INTO {_tableName.QualifiedName} (
    [id],
    [revision],
    [data],
    [metadata]
) VALUES (
    @id, 
    @revision, 
    @data,
    @metadata
)

";
                    command.Parameters.Add("id", SqlDbType.UniqueIdentifier).Value = sagaData.Id;
                    command.Parameters.Add("revision", SqlDbType.Int).Value = sagaData.Revision;
                    command.Parameters.Add("data", SqlDbType.NVarChar).Value = DataSerializer.SerializeToString(sagaData);
                    command.Parameters.Add("metadata", SqlDbType.NVarChar).Value = MetadataSerializer.SerializeToString(sagaAuditMetadata);

                    await command.ExecuteNonQueryAsync();
                }

                await connection.Complete();
            }
        }
    }
}