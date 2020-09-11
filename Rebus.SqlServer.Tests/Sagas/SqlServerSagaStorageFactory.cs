using NUnit.Framework;
using Rebus.Logging;
using Rebus.Sagas;
using Rebus.SqlServer.Sagas;
using Rebus.SqlServer.Sagas.Serialization;
using Rebus.Tests.Contracts.Sagas;

namespace Rebus.SqlServer.Tests.Sagas
{
    [TestFixture, Category(Categories.SqlServer)]
    public class SqlServerSagaStorageBasicLoadAndSaveAndFindOperations : BasicLoadAndSaveAndFindOperations<SqlServerSagaStorageFactory> { }

    [TestFixture, Category(Categories.SqlServer)]
    public class SqlServerSagaStorageConcurrencyHandling : ConcurrencyHandling<SqlServerSagaStorageFactory> { }

    [TestFixture, Category(Categories.SqlServer)]
    public class SqlServerSagaStorageSagaIntegrationTests : SagaIntegrationTests<SqlServerSagaStorageFactory> { }

    public class SqlServerSagaStorageFactory : ISagaStorageFactory
    {
        const string IndexTableName = "RebusSagaIndex";
        const string DataTableName = "RebusSagaData";

        public SqlServerSagaStorageFactory()
        {
            CleanUp();
        }

        public ISagaStorage GetSagaStorage()
        {
            var consoleLoggerFactory = new ConsoleLoggerFactory(true);
            var connectionProvider = new DbConnectionProvider(SqlTestHelper.ConnectionString, consoleLoggerFactory);
            var sagaTypeNamingStrategy = new LegacySagaTypeNamingStrategy();
            var serializer = new DefaultSagaSerializer();
            var storage = new SqlServerSagaStorage(connectionProvider, DataTableName, IndexTableName, consoleLoggerFactory, sagaTypeNamingStrategy, serializer);

            storage.EnsureTablesAreCreated();

            return storage;
        }

        public void CleanUp()
        {
            SqlTestHelper.DropTable(IndexTableName);
            SqlTestHelper.DropTable(DataTableName);
        }
    }
}
