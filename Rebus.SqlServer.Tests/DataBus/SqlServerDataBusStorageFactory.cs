using Rebus.DataBus;
using Rebus.Logging;
using Rebus.SqlServer.DataBus;
using Rebus.Tests.Contracts.DataBus;
using Rebus.Time;

namespace Rebus.SqlServer.Tests.DataBus
{
    public class SqlServerDataBusStorageFactory : IDataBusStorageFactory
    {
        public SqlServerDataBusStorageFactory()
        {
            SqlTestHelper.DropTable("databus");
        }

        public IDataBusStorage Create()
        {
            var rebusTime = new DefaultRebusTime();
            var consoleLoggerFactory = new ConsoleLoggerFactory(false);
            var connectionProvider = new DbConnectionProvider(SqlTestHelper.ConnectionString, consoleLoggerFactory);
            var sqlServerDataBusStorage = new SqlServerDataBusStorage(connectionProvider, "databus", true, consoleLoggerFactory, rebusTime, 240);
            sqlServerDataBusStorage.Initialize();
            return sqlServerDataBusStorage;
        }

        public void CleanUp()
        {
            SqlTestHelper.DropTable("databus");
        }
    }
}