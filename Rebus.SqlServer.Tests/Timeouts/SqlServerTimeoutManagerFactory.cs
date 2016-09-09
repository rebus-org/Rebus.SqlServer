using NUnit.Framework;
using Rebus.Logging;
using Rebus.SqlServer.Timeouts;
using Rebus.Tests.Contracts.Timeouts;
using Rebus.Timeouts;

namespace Rebus.SqlServer.Tests.Timeouts
{
    [TestFixture, Category(Categories.SqlServer)]
    public class BasicStoreAndRetrieveOperations : BasicStoreAndRetrieveOperations<SqlServerTimeoutManagerFactory>
    {
    }

    public class SqlServerTimeoutManagerFactory : ITimeoutManagerFactory
    {
        const string TableName = "RebusTimeouts";

        public ITimeoutManager Create()
        {
            var consoleLoggerFactory = new ConsoleLoggerFactory(true);
            var connectionProvider = new DbConnectionProvider(SqlTestHelper.ConnectionString, consoleLoggerFactory);
            var timeoutManager = new SqlServerTimeoutManager(connectionProvider, TableName, consoleLoggerFactory);

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
    }
}