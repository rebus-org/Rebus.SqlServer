using System;
using Rebus.Logging;
using Rebus.SqlServer.Timeouts;
using Rebus.Tests.Contracts.Timeouts;
using Rebus.Timeouts;

namespace Rebus.SqlServer.Tests.Timeouts
{
    public class SqlServerTimeoutManagerFactory : ITimeoutManagerFactory
    {
        const string TableName = "RebusTimeouts";

        readonly FakeRebusTime _fakeRebusTime = new FakeRebusTime();

        public ITimeoutManager Create()
        {
            var consoleLoggerFactory = new ConsoleLoggerFactory(true);
            var connectionProvider = new DbConnectionProvider(SqlTestHelper.ConnectionString, consoleLoggerFactory);
            var timeoutManager = new SqlServerTimeoutManager(connectionProvider, TableName, consoleLoggerFactory, _fakeRebusTime);

            timeoutManager.EnsureTableIsCreated();

            return timeoutManager;
        }

        public void Cleanup()
        {
            SqlTestHelper.DropTable(TableName);
        }

        public string GetDebugInfo()
        {
            return "could not provide debug info for this particular timeout manager.... implement if needed :)";
        }

        public void FakeIt(DateTimeOffset fakeTime)
        {
            _fakeRebusTime.SetNow(fakeTime);
        }
    }
}