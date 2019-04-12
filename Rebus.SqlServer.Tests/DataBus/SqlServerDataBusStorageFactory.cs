using System;
using Rebus.DataBus;
using Rebus.Logging;
using Rebus.SqlServer.DataBus;
using Rebus.Tests.Contracts.DataBus;

namespace Rebus.SqlServer.Tests.DataBus
{
    public class SqlServerDataBusStorageFactory : IDataBusStorageFactory
    {
        readonly FakeRebusTime _fakeRebusTime = new FakeRebusTime();
        
        public SqlServerDataBusStorageFactory()
        {
            SqlTestHelper.DropTable("databus");
        }
        
        public IDataBusStorage Create()
        {
            var consoleLoggerFactory = new ConsoleLoggerFactory(false);
            var connectionProvider = new DbConnectionProvider(SqlTestHelper.ConnectionString, consoleLoggerFactory);
            var sqlServerDataBusStorage = new SqlServerDataBusStorage(connectionProvider, "databus", true, consoleLoggerFactory, _fakeRebusTime, 240);
            sqlServerDataBusStorage.Initialize();
            return sqlServerDataBusStorage;
        }

        public void CleanUp()
        {
            SqlTestHelper.DropTable("databus");
        }

        public void FakeIt(DateTimeOffset fakeTime)
        {
            _fakeRebusTime.SetNow(fakeTime);
        }
    }
}